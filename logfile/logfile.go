package logfile

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Entry is a log entry from a system, e.g. CloudWatch, which might look like:
// 2018-03-29T14:40:23.417Z {"time":"2018-03-29T14:40:23Z","src":"rl","status":200,"http_2xx":1,"len":11,"ms":162,"path":"/user/details"}
type Entry struct {
	// The time of the log entry.
	Time time.Time
	// The log message.
	Message string
}

// EntryFromCloudwatch gets an entry from Cloudwatch.
func EntryFromCloudwatch(s string) (e Entry, ok bool) {
	ts := string(s[:24])
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return
	}
	m := s[25:]
	e = Entry{
		Time:    t,
		Message: m,
	}
	ok = true
	return
}

// ReaderStats provides statistics about how much was read.
type ReaderStats struct {
	FilesRead int
	LinesRead int
	Extracted int
}

// NewCloudWatchReader creates a reader which can read CloudWatch logs from disk.
func NewCloudWatchReader(path string) CloudWatchReader {
	return CloudWatchReader{
		Path: path,
	}
}

// CloudWatchReader allows reading *.gz CloudWatch logs from a directory on disk.
type CloudWatchReader struct {
	Path string
}

// Read the directory recursively and send entries to the channel.
func (cwr CloudWatchReader) Read(lec chan Entry) (rs ReaderStats, err error) {
	fileProcessor := func(path string, info os.FileInfo, inerr error) (err error) {
		rs.FilesRead++
		if inerr != nil {
			return inerr
		}
		if info.IsDir() {
			return
		}
		if !strings.HasSuffix(info.Name(), ".gz") {
			return
		}
		f, err := os.Open(path)
		if err != nil {
			return
		}
		defer f.Close()
		zr, err := gzip.NewReader(f)
		if err != nil {
			return
		}
		defer zr.Close()
		lr, ee, err := ReadCloudwatch(zr, lec)
		rs.LinesRead += lr
		rs.Extracted += ee
		return err
	}
	err = filepath.Walk(cwr.Path, fileProcessor)
	return
}

// ReadCloudwatch reads a CloudWatch gz file and sends log entries to the channel.
func ReadCloudwatch(r io.Reader, c chan Entry) (linesRead, entriesExtracted int, err error) {
	reader := bufio.NewReader(r)

	var line string
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		linesRead++
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if le, ok := EntryFromCloudwatch(line); ok {
			c <- le
			entriesExtracted++
		}
	}
	if err == io.EOF {
		err = nil
	}
	return
}
