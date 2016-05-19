// Copyright 2014 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Package main implements cloudsql-import, a program resilient to
// restarts that replays a mysql dump to a MySQL server. The resilience
// is gained by saving the current state after each query.
package main

// TODO: save the /*!... */ queries and replay them when restarting from a checkpoint.
// TODO: speed up the replay by issuing queries concurrently.

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/ssh/terminal"
)

var (
	dump       = flag.String("dump", "", "MySQL dump file")
	dsn        = flag.String("dsn", "root:root@tcp(0.0.0.0:3306)/", "MySQL Data Source Name")
	enableSsl  = flag.Bool("enable_ssl", false, "Connect to MySQL with SSL")
	prompt     = flag.Bool("prompt", false, "Prompt for password rather than specifying in the command")
	sslCa      = flag.String("ssl_ca", "server-ca.pem", "MySQL Server certificate")
	sslCert    = flag.String("ssl_cert", "client-cert.pem", "MySQL Client PEM cert file")
	sslKey     = flag.String("ssl_key", "client-key.pem", "MySQL Client PEM key file")
	serverName = flag.String("server_name", "project:instance", "Cloud SQL project and instance name")
)

type logLine struct {
	Position int64
}

// recover recovers the last checkpoint offset.
func recover(filename string) (int64, error) {
	f, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	last := int64(0)
	for s.Scan() {
		ll := &logLine{}
		err = json.Unmarshal(s.Bytes(), ll)
		if err != nil {
			return 0, err
		}
		last = ll.Position
	}
	if err := s.Err(); err != nil {
		return 0, err
	}
	return last, nil
}

func save(f *os.File, pos int64) error {
	b, err := json.Marshal(logLine{Position: pos})
	if err != nil {
		return err
	}
	_, err = f.Write(append(b, '\n'))
	if err != nil {
		return err
	}
	return f.Sync()
}

// replay replays a MySQL line. Returns false if more data is needed.
func replay(db *sql.DB, line []byte, pos int64, size int64) bool {
	// A comment line starts either with "#" or a "-- ". A "--" is
	// also a valid comment line. A regular line ends with a ";",
	//
	// Reference: http://dev.mysql.com/doc/refman/5.5/en/comments.html
	if len(line) == 0 ||
		bytes.Equal(line, []byte("--")) ||
		bytes.HasPrefix(line, []byte("-- ")) ||
		bytes.HasPrefix(line, []byte("#")) {
		return true
	}

	if line[len(line)-1] != ';' {
		return false
	}

	s := string(line)
	start := time.Now()
	_, err := db.Exec(s)
	since := time.Since(start)
	if len(s) > 80 {
		s = s[:60] + "[...]" + s[len(s)-10:]
	}
	log.Printf("%.2f %7dms %7d %q", float64(pos)/float64(size), since/time.Millisecond, len(line), s)

	if err != nil {
		if merr, ok := err.(*mysql.MySQLError); ok && merr.Number == 1062 {
			log.Printf(`ignoring "duplicate entry" error`)
		} else {
			log.Fatal(err)
		}
	}

	return true
}

func main() {
	flag.Parse()

	if *dump == "" {
		log.Fatalf("no -dump file specified")
	}

	if *enableSsl {
		rootCertPool := x509.NewCertPool()
		pem, err := ioutil.ReadFile(*sslCa)
		if err != nil {
			log.Fatalln("ioutil.Readline:", err)
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			log.Fatal("Failed to append CA certificate PEM.")
		}
		clientCert := []tls.Certificate{}
		certs, err := tls.LoadX509KeyPair(*sslCert, *sslKey)
		if err != nil {
			log.Fatalln("tls.LoadX509KeyPair:", err)
		}
		clientCert = append(clientCert, certs)
		mysql.RegisterTLSConfig("custom", &tls.Config{
			RootCAs:      rootCertPool,
			Certificates: clientCert,
			ServerName:   *serverName,
		})
	}

	var finalDsn = *dsn
	if *prompt {
		dsnRegex := regexp.MustCompile(`(\w*):?\w*(@.+)`)
		matches := dsnRegex.FindStringSubmatch(finalDsn)
		if matches == nil {
			fmt.Print("Incorrect format for dsn. Usage:\n")
			flag.PrintDefaults()
			os.Exit(1)
		}

		fmt.Print("Enter password: ")
		// don't echo password to screen during input
		bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			log.Fatalln("Error reading password:", err)
		}
		// ReadPassword() leaves cursor on the input line,
		// so begin output on the next line
		fmt.Print("\n")

		// insert password into the connection string
		finalDsn = strings.Join([]string{matches[1], ":", string(bytePassword), matches[2]}, "")
	}

	db, err := sql.Open("mysql", finalDsn)
	if err != nil {
		log.Fatalln("sql.Open:", err)
	}
	defer db.Close()

	f, err := os.Open(*dump)
	if err != nil {
		log.Fatalf("os.Open: %v", err)
	}
	defer f.Close()
	dumpInfo, err := f.Stat()
	if err != nil {
		log.Fatalf("Stat: %v", err)
	}
	size := dumpInfo.Size()

	logFilename := fmt.Sprintf("%s.log", dumpInfo.Name())
	pos, err := recover(logFilename)
	if err != nil {
		log.Fatalf("recover from log: %v", err)
	}
	if pos != 0 {
		log.Printf("seeking to %d in %q", pos, f.Name())
		if _, err = f.Seek(pos, os.SEEK_SET); err != nil {
			log.Fatalf("Seek: %v", err)
		}
	}

	logFile, err := os.OpenFile(logFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("os.OpenFile: %v", err)
	}
	defer logFile.Close()

	// buf[i:j] are the bytes that have been read from f but not
	// yet replayed. k indicates up to where we read in a
	// multi-line query.
	buf := make([]byte, 1024*1024)
	i, j, k, readErr := 0, 0, 0, error(nil)
	for {
		if p := bytes.IndexByte(buf[k:j], '\n'); p >= 0 {
			k += p + 1 // The +1 is for the trailing '\n'.
			pos += int64(p + 1)
			if replay(db, buf[i:k-1], pos, size) {
				i = k
				err = save(logFile, pos)
				if err != nil {
					log.Fatalf("Error saving to log: %v", err)
				}
			}
			continue
		}

		if readErr != nil {
			if readErr == io.EOF {
				if i != j {
					log.Println(i, j)
					log.Fatalf(`The contents of %q do not end with a "\n"`, *dump)
				}
				return
			}
			log.Fatal(readErr)
		}

		// First, make sure at least half of the buffer is empty. If we can do
		// that by moving the contents of buf[i:j] to the start of the existing
		// buffer, that's great. Otherwise, allocate a bigger buffer.
		newBuf := buf
		if j-i > len(buf)/2 {
			newBuf = make([]byte, len(buf)*2)
		}
		i, j, k = 0, copy(newBuf, buf[i:j]), k-i
		buf = newBuf
		// Read some more bytes.
		var n int
		n, readErr = f.Read(buf[j:])
		j += n
	}
}
