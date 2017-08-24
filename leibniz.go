package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/OneOfOne/xxhash"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var createDbStmt string = `
	create table roots (id integer not null primary key, root text);
	create table files (id integer not null primary key, root_id integer, hash text, path string, mtime datetime);
	`

var createIdxStmt string = `
	create unique index if not exists unique_root_idx on roots (root);
	create index if not exists root_idx on files (root_id);
	create index if not exists hash_idx on files (hash);
	`

type RegexFlag []*regexp.Regexp

func (e *RegexFlag) String() string {
	var parts []string
	if e == nil {
		return ""
	}

	for _, re := range *e {
		parts = append(parts, re.String())
	}

	return strings.Join(parts, ", ")
}

func (e *RegexFlag) Set(value string) error {
	excludeRe, err := regexp.Compile(value)
	if err != nil {
		return err
	}

	*e = append(*e, excludeRe)

	return nil
}

func (e *RegexFlag) Match(s string) bool {
	for _, re := range *e {
		if re.MatchString(s) {
			return true
		}
	}

	return false
}

type Options struct {
	root        string
	catalogPath string
	excludes    *RegexFlag
	includes    *RegexFlag
	hashFile    string
	verbose     bool
}

func parseOptions() *Options {
	home := os.Getenv("HOME")
	root := flag.String("root", home, "Catalog all files in this directory")
	verbosity := flag.Bool("verbose", false, "Be chattier")
	catalogPath := flag.String("catalog", path.Join(home, ".leibniz-catalog"), "Path to the catalog file")
	var excludes RegexFlag
	var includes RegexFlag
	flag.Var(&excludes, "exclude", "Exclude paths that match this regex. Excludes are tested before includes")
	flag.Var(&includes, "include", "Include paths that match this regex")
	hashFile := flag.String("singleton", "", "Hash a single file")

	flag.Parse()

	if root == nil || *root == "" || catalogPath == nil || *catalogPath == "" {
		flag.Usage()
		return nil
	}

	for _, re := range excludes {
		fmt.Println("Excluding:", re.String())
	}

	return &Options{*root, *catalogPath, &excludes, &includes, *hashFile, *verbosity}
}

type Catalog struct {
	Db   *sql.DB
	Opts *Options
}

func (c *Catalog) Verbosity(fmtstr string, vars ...interface{}) {
	if c.Opts.verbose {
		fmt.Printf(fmtstr, vars...)
	}
}

func OpenCatalog(options *Options) (*Catalog, error) {
	db, err := sql.Open("sqlite3", options.catalogPath)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createDbStmt)
	if err != nil && err.Error() != "table roots already exists" {
		db.Close()
		return nil, err
	}

	_, err = db.Exec(createIdxStmt)
	if err != nil {
		db.Close()
		return nil, err
	}

	return &Catalog{db, options}, nil
}

// A get-or-insert command that always maintains the roots table
func (c *Catalog) EnsureRootId(root string) (int64, error) {
	var existingRoot string
	var rootId int64
	err := c.Db.QueryRow(`select id, root from roots where root=?`, root).Scan(&rootId, &existingRoot)

	switch {
	case err == sql.ErrNoRows:
		res, err := c.Db.Exec(`insert into roots (root) values (?)`, root)
		if err != nil {
			return -1, err
		}

		return res.LastInsertId()
	case err != nil:
		return -1, err
	default:
		return rootId, nil
	}
}

func (c *Catalog) CatalogHash(rootId int64, hash uint64, path string, mtime time.Time) (int64, error) {
	hashString := fmt.Sprintf("%x", hash)
	res, err := c.Db.Exec(`insert into files (root_id, hash, path, mtime) values (?, ?, ?, ?)`, rootId, hashString, path, mtime)
	if err != nil {
		return -1, err
	}

	return res.LastInsertId()
}

func (c *Catalog) HashAndCatalog(rootId int64, walked WalkerContext) error {
	realpath := path.Join(walked.Context, walked.Info.Name())

	file, err := os.Open(realpath)
	if err != nil {
		pathErr, ok := err.(*os.PathError)
		if !ok {
			return fmt.Errorf("not a PathError!")
		}

		if pathErr.Err.Error() == "permission denied" {
			fmt.Println("Permission denied:", realpath)
			return nil
		}
		return err
	}
	defer file.Close()

	smartHash, err := SmartHash(file, walked.Info, 512*1024)
	if err != nil {
		return fmt.Errorf("%s: %s", realpath, err.Error())
	}

	c.CatalogHash(rootId, smartHash, realpath, walked.Info.ModTime())

	c.Verbosity("Cataloged %s: %x\n", realpath, smartHash)

	return nil
}

type WalkerContext struct {
	Info    os.FileInfo
	Context string
}

func (c *Catalog) Run() error {
	root := c.Opts.root

	rootInfo, err := os.Stat(root)
	if err != nil {
		return err
	}

	if !rootInfo.IsDir() {
		return fmt.Errorf("Root (%s) is not a directory.", root)
	}

	rootId, err := c.EnsureRootId(root)
	if err != nil {
		return err
	}

	// Non-recursive directory walk
	fileQ := make([]WalkerContext, 0)
	fileQ = append(fileQ, WalkerContext{rootInfo, path.Dir(root)})
	var cur WalkerContext
	for {
		if len(fileQ) < 1 {
			break
		}

		cur, fileQ = fileQ[0], fileQ[1:]
		context := path.Join(cur.Context, cur.Info.Name())

		if cur.Info.IsDir() {
			dir, err := os.Open(context)
			if err != nil {
				return err
			}

			infos, err := dir.Readdir(0)
			if err != nil {
				dir.Close()
				return err
			}

			for _, info := range infos {
				realpath := path.Join(context, info.Name())
				if c.Opts.excludes.Match(realpath) {
					c.Verbosity("Skipping %s\n", realpath)
					continue
				}

				fileQ = append(fileQ, WalkerContext{info, context})
			}

			dir.Close()

			continue
		}

		switch {
		case !cur.Info.Mode().IsRegular():
			continue
		case len(*c.Opts.includes) > 0 && !c.Opts.includes.Match(context):
			continue
		default:
			err = c.HashAndCatalog(rootId, cur)
			if err != nil {
				return err
			}
			break
		}
	}

	return nil
}

func fullHash(file *os.File, size int64) ([]byte, error) {
	xx := xxhash.New64()
	_, err := io.Copy(xx, file)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, []uint64{xx.Sum64(), uint64(size)})
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// We take 1k samples from the start, middle, and end of the file
// File should be big enough that size / 2 > 1024 and size - 1024 > (size / 2) + 1024
// But really a file of at least 3k will work
func sampleHash(file *os.File, size int64) ([]byte, error) {
	offsets := []int64{
		0,
		size / 2,
		size - 1024,
	}

	xx := xxhash.New64()
	var err error
	for i, offset := range offsets {
		buf := make([]byte, 1024)
		_, err = file.ReadAt(buf, offset)
		if err == io.EOF && i < len(offsets)-1 {
			return nil, fmt.Errorf("Unexpected EOF!")
		}

		xx.Write(buf)
	}

	if err != nil && err != io.EOF {
		return nil, err
	}

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, []uint64{xx.Sum64(), uint64(size)})
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func SmartHash(file *os.File, info os.FileInfo, threshold int64) (uint64, error) {
	var xxSum []byte
	var err error

	if info.Size() < threshold {
		xxSum, err = fullHash(file, info.Size())
	} else {
		xxSum, err = sampleHash(file, info.Size())
	}

	if err != nil {
		return 0, err
	}

	xx := xxhash.New64()
	xx.Write(xxSum)

	return xx.Sum64(), nil
}

func singleton(file string) {
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}

	finfo, err := f.Stat()
	if err != nil {
		panic(err)
	}

	hash, err := SmartHash(f, finfo, 512*1024)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%v (%x)\n", hash, hash)
}

func main() {
	options := parseOptions()
	if options == nil {
		return
	}

	if len(options.hashFile) > 0 {
		singleton(options.hashFile)
		return
	}

	absroot, err := filepath.Abs(options.root)
	if err != nil {
		panic(err)
	}
	options.root = absroot

	catalog, err := OpenCatalog(options)
	if err != nil {
		panic(err)
	}

	catalog.Verbosity("Cataloging %s\n", options.root)
	err = catalog.Run()
	if err != nil {
		panic(err)
	}
}
