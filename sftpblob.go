package sftpblob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/sftp"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"
	"golang.org/x/crypto/ssh"
)

const defaultPageSize = 1000

var (
	errNotFound       = errors.New("blob not found")
	errNotImplemented = errors.New("not implemented")
)

func init() {
	blob.DefaultURLMux().RegisterBucket(Scheme, &URLOpener{})
}

// Scheme is the URL scheme sftpblob registers its URLOpener under on
// blob.DefaultMux.
const Scheme = "sftp"

// URLOpener opens file bucket URLs like "sftp://test:test@127.0.0.1/foo/bar/baz"
type URLOpener struct {
	// Options specifies the default options to pass to OpenBucket.
	Options Options
}

// OpenBucketURL opens a blob.Bucket based on u.
func (o *URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	opts := &Options{}
	return OpenBucket(u, opts)
}

// Options sets options for constructing a *blob.Bucket backed by fileblob.
type Options struct {
}

type bucket struct {
	dir        string
	opts       *Options
	sftpClient *sftp.Client
}

// openBucket creates a driver.Bucket that reads and writes to dir.
// dir must exist.
func openBucket(u *url.URL, opts *Options) (driver.Bucket, error) {
	pass, _ := u.User.Password()
	sshConfig := &ssh.ClientConfig{
		User: u.User.Username(),
		Auth: []ssh.AuthMethod{
			ssh.Password(pass),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	client, err := ssh.Dial("tcp", u.Host, sshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to sftp dial to the host %s: %v", u.Host, err)
	}
	// open an SFTP session over an existing ssh connection.
	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create sftp client for the host %s: %v", u.Host, err)
	}

	dir := u.Path
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", dir)
	}
	if opts == nil {
		opts = &Options{}
	}
	return &bucket{
		dir:        dir,
		opts:       opts,
		sftpClient: sftpClient,
	}, nil
}

// OpenBucket creates a *blob.Bucket backed by the sftp
func OpenBucket(u *url.URL, opts *Options) (*blob.Bucket, error) {
	drv, err := openBucket(u, opts)
	if err != nil {
		return nil, err
	}
	return blob.NewBucket(drv), nil
}

func (b *bucket) Close() error {
	if b.sftpClient != nil {
		return b.sftpClient.Close()
	}
	return nil
}

func (b *bucket) ErrorCode(err error) gcerrors.ErrorCode {
	switch {
	case os.IsNotExist(err):
		return gcerrors.NotFound
	default:
		return gcerrors.Unknown
	}
}

// path returns the full path for a key
func (b *bucket) path(key string) (string, error) {
	path := filepath.Join(b.dir, key)
	return path, nil
}

// forKey returns the full path, os.FileInfo, and attributes for key.
func (b *bucket) forKey(key string) (string, os.FileInfo, error) {
	path, err := b.path(key)
	if err != nil {
		return "", nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", nil, err
	}
	if info.IsDir() {
		return "", nil, os.ErrNotExist
	}
	return path, info, nil
}

// ListPaged implements driver.ListPaged.
func (b *bucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {

	var pageToken string
	if len(opts.PageToken) > 0 {
		pageToken = string(opts.PageToken)
	}
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}
	// If opts.Delimiter != "", lastPrefix contains the last "directory" key we
	// added. It is used to avoid adding it again; all files in this "directory"
	// are collapsed to the single directory entry.
	var lastPrefix string

	// If the Prefix contains a "/", we can set the root of the Walk
	// to the path specified by the Prefix as any files below the path will not
	// match the Prefix.
	// Note that we use "/" explicitly and not os.PathSeparator, as the opts.Prefix
	// is in the unescaped form.
	root := b.dir
	if i := strings.LastIndex(opts.Prefix, "/"); i > -1 {
		root = filepath.Join(root, opts.Prefix[:i])
	}

	// Do a full recursive scan of the root directory.
	var result driver.ListPage
	walker := b.sftpClient.Walk(root)
	for walker.Step() {
		err := walker.Err()
		if err != nil {
			continue
		}
		path := walker.Path()
		// os.Walk returns the root directory; skip it.
		if path == b.dir {
			continue
		}

		info := walker.Stat()
		// Strip the <b.dir> prefix from path; +1 is to include the separator.
		path = path[len(b.dir):]
		// Unescape the path to get the key.
		key := path
		// Skip all directories. If opts.Delimiter is set, we'll create
		// pseudo-directories later.
		// Note that returning nil means that we'll still recurse into it;
		// we're just not adding a result for the directory itself.
		if info.IsDir() {
			key += "/"
			// Avoid recursing into subdirectories if the directory name already
			// doesn't match the prefix; any files in it are guaranteed not to match.
			if len(key) > len(opts.Prefix) && !strings.HasPrefix(key, opts.Prefix) {
				walker.SkipDir()
			}
			// Similarly, avoid recursing into subdirectories if we're making
			// "directories" and all of the files in this subdirectory are guaranteed
			// to collapse to a "directory" that we've already added.
			if lastPrefix != "" && strings.HasPrefix(key, lastPrefix) {
				walker.SkipDir()
			}
			continue
		}
		// Skip files/directories that don't match the Prefix.
		if !strings.HasPrefix(key, opts.Prefix) {
			continue
		}
		obj := &driver.ListObject{
			Key:     key,
			ModTime: info.ModTime(),
			Size:    info.Size(),
		}
		// If using Delimiter, collapse "directories".
		if opts.Delimiter != "" {
			// Strip the prefix, which may contain Delimiter.
			keyWithoutPrefix := key[len(opts.Prefix):]
			// See if the key still contains Delimiter.
			// If no, it's a file and we just include it.
			// If yes, it's a file in a "sub-directory" and we want to collapse
			// all files in that "sub-directory" into a single "directory" result.
			if idx := strings.Index(keyWithoutPrefix, opts.Delimiter); idx != -1 {
				prefix := opts.Prefix + keyWithoutPrefix[0:idx+len(opts.Delimiter)]
				// We've already included this "directory"; don't add it.
				if prefix == lastPrefix {
					continue
				}
				// Update the object to be a "directory".
				obj = &driver.ListObject{
					Key:   prefix,
					IsDir: true,
				}
				lastPrefix = prefix
			}
		}
		// If there's a pageToken, skip anything before it.
		if pageToken != "" && obj.Key <= pageToken {
			continue
		}
		// If we've already got a full page of results, set NextPageToken and stop.
		if len(result.Objects) == pageSize {
			result.NextPageToken = []byte(result.Objects[pageSize-1].Key)
			break
		}
		result.Objects = append(result.Objects, obj)
	}
	return &result, nil
}

// As implements driver.As.
func (b *bucket) As(i interface{}) bool { return false }

// As implements driver.ErrorAs.
func (b *bucket) ErrorAs(err error, i interface{}) bool {
	if perr, ok := err.(*os.PathError); ok {
		if p, ok := i.(**os.PathError); ok {
			*p = perr
			return true
		}
	}
	return false
}

// Attributes implements driver.Attributes.
func (b *bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	_, info, err := b.forKey(key)
	if err != nil {
		return nil, err
	}
	return &driver.Attributes{
		ModTime: info.ModTime(),
		Size:    info.Size(),
	}, nil
}

// NewRangeReader implements driver.NewRangeReader.
func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	path, info, err := b.forKey(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if opts.BeforeRead != nil {
		if err := opts.BeforeRead(func(interface{}) bool { return false }); err != nil {
			return nil, err
		}
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}
	r := io.Reader(f)
	if length >= 0 {
		r = io.LimitReader(r, length)
	}
	return &reader{
		r: r,
		c: f,
		attrs: driver.ReaderAttributes{
			ModTime: info.ModTime(),
			Size:    info.Size(),
		},
	}, nil
}

type reader struct {
	r     io.Reader
	c     io.Closer
	attrs driver.ReaderAttributes
}

func (r *reader) Read(p []byte) (int, error) {
	if r.r == nil {
		return 0, io.EOF
	}
	return r.r.Read(p)
}

func (r *reader) Close() error {
	if r.c == nil {
		return nil
	}
	return r.c.Close()
}

func (r *reader) Attributes() *driver.ReaderAttributes {
	return &r.attrs
}

func (r *reader) As(i interface{}) bool { return false }

// NewTypedWriter implements driver.NewTypedWriter.
func (b *bucket) NewTypedWriter(ctx context.Context, key string, contentType string, opts *driver.WriterOptions) (driver.Writer, error) {
	path, err := b.path(key)
	if err != nil {
		return nil, err
	}
	if err := b.sftpClient.MkdirAll(filepath.Dir(path)); err != nil {
		return nil, err
	}
	f, err := b.sftpClient.Create(path)
	if err != nil {
		return nil, err
	}
	if opts.BeforeWrite != nil {
		if err := opts.BeforeWrite(func(interface{}) bool { return false }); err != nil {
			return nil, err
		}
	}
	w := &writer{
		ctx:  ctx,
		f:    f,
		path: path,
	}
	return w, nil
}

type writer struct {
	ctx  context.Context
	f    *sftp.File
	path string
}

func (w *writer) Write(p []byte) (n int, err error) {
	return w.f.Write(p)
}

func (w *writer) Close() error {
	err := w.f.Close()
	if err != nil {
		return err
	}

	// Check if the write was cancelled.
	if err := w.ctx.Err(); err != nil {
		return err
	}

	return nil
}

// Copy implements driver.Copy.
func (b *bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *driver.CopyOptions) error {
	// Note: we could use NewRangeReader here, but since we need to copy all of
	// the metadata (from xa), it's more efficient to do it directly.
	srcPath, _, err := b.forKey(srcKey)
	if err != nil {
		return err
	}
	f, err := b.sftpClient.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// We'll write the copy using Writer, to avoid re-implementing making of a
	// temp file, cleaning up after partial failures, etc.
	wopts := driver.WriterOptions{
		BeforeWrite: opts.BeforeCopy,
	}
	// Create a cancelable context so we can cancel the write if there are
	// problems.
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := b.NewTypedWriter(writeCtx, dstKey, "", &wopts)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, f)
	if err != nil {
		cancel() // cancel before Close cancels the write
		w.Close()
		return err
	}
	return w.Close()
}

// Delete implements driver.Delete.
func (b *bucket) Delete(ctx context.Context, key string) error {
	path, err := b.path(key)
	if err != nil {
		return err
	}
	err = os.Remove(path)
	if err != nil {
		return err
	}
	return nil
}

func (b *bucket) SignedURL(ctx context.Context, key string, opts *driver.SignedURLOptions) (string, error) {
	return "", errNotImplemented
}
