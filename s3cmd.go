package main

import (
	"flag"
	"fmt"
	"log"
	"io"
	"os"
	"strings"

	gos3 "github.com/takoyaki-3/go-s3"
	"compress/gzip"
	"archive/tar"
	"path/filepath"
)

func main(){
	flag.Parse()
	args := flag.Args()

	if len(args) < 2 {
		log.Fatalln("Args is not enough.")
	}

	// 実行ファイルのパスを取得
	executablePath, err := os.Executable()
	if err != nil {
		log.Fatalf("failed to find executable path: %v", err)
	}

	// 実行ファイルのディレクトリを取得
	executableDir := filepath.Dir(executablePath)

	// 相対パスを指定してファイルパスを作成
	filePath := filepath.Join(executableDir, "s3-conf.json")
	
	s3,err := gos3.NewSession(filePath)
	if err!=nil{
		log.Fatalln(err)
	}

	switch(args[0]){
	case "ls":
		ls(&s3,args[1])
		break
	case "upload":
		targzUpload(&s3,args[1],args[2])
		break
	case "download":
		targzDownload(&s3,args[1],args[2])
		break
	}
}

func ls(s3 *gos3.Session,dir string)error{
	if list,err:=s3.GetObjectList(dir);err!=nil{
		return err
	} else {
		for _,v:=range list{
			fmt.Println(v)
		}
	}
	return nil
}

func upload(s3 *gos3.Session,org, dst string)error{
	if err:=s3.UploadFromPath(org,dst);err!=nil{
		return err
	}
	fmt.Println("["+org+"] upload done.")
	return nil
}

func download(s3 *gos3.Session,org,dst string)error{
	return s3.DownloadToReaderFunc(org,func(r io.Reader) error {
		f, err := os.Create(dst)
		if err!=nil{
			return err
		}
		_, err = io.Copy(f,r)
		if err!=nil{
			return err
		}
		return nil
	})
}

func targzUpload(s3 *gos3.Session, org, dst string) error {
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		gw := gzip.NewWriter(pw)
		defer gw.Close()

		tw := tar.NewWriter(gw)
		defer tw.Close()

		err := filepath.Walk(org, func(path string, info os.FileInfo, err error) error {
			if info.IsDir() {
				return nil
			}

			if err := tw.WriteHeader(&tar.Header{
				Name:    path,
				Mode:    int64(info.Mode()),
				ModTime: info.ModTime(),
				Size:    info.Size(),
			}); err != nil {
				return err
			}

			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()
			if _, err := io.Copy(tw, f); err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			log.Fatalln(err)
		}
	}()

	if err := s3.UploadFromReader(pr, dst); err != nil {
		log.Fatalln(err)
		return err
	}

	return nil
}

func targzDownload(s3 *gos3.Session, org, dst string) error {
	return s3.DownloadToReaderFunc(org, func(r io.Reader) error {
		// gzipの展開
		gzipReader, err := gzip.NewReader(r)
		defer gzipReader.Close()
		if err != nil {
			return err
		}

		// tarの展開
		tarReader := tar.NewReader(gzipReader)

		for {
			tarHeader, err := tarReader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			// ￥の区切り文字をOS依存の区切り文字に置き換える
			cleanName := strings.ReplaceAll(tarHeader.Name, "\\", string(os.PathSeparator))
			
			// ファイルパスを結合する際に filepath.Join を使用
			fullPath := filepath.Join(dst, cleanName)
			dir := filepath.Dir(fullPath)

			if err := os.MkdirAll(dir, 0777); err != nil {
				return err
			}

			f, err := os.Create(fullPath)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tarReader); err != nil {
				f.Close()
				return err
			}
			f.Close()
		}
		return nil
	})
}