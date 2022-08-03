package main

import (
	"flag"
	"fmt"
	"log"
	"io"
	"os"

	gos3 "github.com/takoyaki-3/go-s3"
)

func main(){
	flag.Parse()
	args := flag.Args()

	if len(args) < 2 {
		log.Fatalln("Args is not enough.")
	}

	s3,err := gos3.NewSession("s3-conf.json")
	if err!=nil{
		log.Fatalln(err)
	}

	switch(args[0]){
	case "ls":
		ls(&s3,args[1])
	case "upload":
		upload(&s3,args[1],args[2])
	case "download":
		download(&s3,args[1],args[2])
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

// func tar(){
// 	dist, err := os.Create(s.Dir+"/"+key+".tar.gz")
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer dist.Close()

// 	gw := gzip.NewWriter(dist)
// 	defer gw.Close()

// 	tw := tar.NewWriter(gw)
// 	defer tw.Close()

// 	// 再帰的にファイルを取得する
// 	if err := filepath.Walk(orginDir, func(path string, info os.FileInfo, err error) error {
// 		// path = strings.Replace(path,"\\","/",-1)
// 		if err != nil {
// 			return err
// 		}
// 		if FileName2IntegratedFileName(path)[:s.Digit] != key {
// 			return nil
// 		}

// 		// ディレクトリは無視
// 		if info.IsDir() {
// 			return nil
// 		}

// 		// ヘッダを書き込み
// 		if err := tw.WriteHeader(&tar.Header{
// 			Name:    path,
// 			Mode:    int64(info.Mode()),
// 			ModTime: info.ModTime(),
// 			Size:    info.Size(),
// 		}); err != nil {
// 			return err
// 		}

// 		// ファイルを書き込み
// 		f, err := os.Open(path)
// 		if err != nil {
// 			return err
// 		}
// 		defer f.Close()
// 		if _, err := io.Copy(tw, f); err != nil {
// 			return err
// 		}

// 		return nil
// 	}); err != nil {
// 		panic(err)
// 	}
// }