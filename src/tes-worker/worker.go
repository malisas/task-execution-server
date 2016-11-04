package main

import (
	"flag"
	uuid "github.com/nu7hatch/gouuid"
	"google.golang.org/grpc"
	"log"
	"os"
	"path/filepath"
	"strings"
	"tes/server/proto"
	"tes/worker"
	"time"
)

func main() {
	agroServer := flag.String("master", "localhost:9090", "Master Server")
	volumeDirArg := flag.String("volumes", "volumes", "Volume Dir")
	storageDirArg := flag.String("storage", "storage", "Storage Dir")
	fileSystemArg := flag.String("files", "", "Allowed File Paths")
	// TODO: swiftDirArg value does not change behaviour of the code
	swiftDirArg := flag.String("swift", "", "Cache Swift items in directory")
	timeoutArg := flag.Int("timeout", -1, "Timeout in seconds")

	// TODO: nworkers is not used
	nworker := flag.Int("nworkers", 4, "Worker Count")
	flag.Parse()
	// volumeDir is the absolute path of volumeDirArg
	volumeDir, _ := filepath.Abs(*volumeDirArg)
	// Check if volumeDir exists. If it doesn't, create it.
	// TODO: see if this can be done without os.Stat, which returns extra information.
	if _, err := os.Stat(volumeDir); os.IsNotExist(err) {
		os.Mkdir(volumeDir, 0700)
	}

	log.Println("Connecting GA4GH Task Server")
	conn, err := grpc.Dial(*agroServer, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	// schedClient is a new SchedulerClient, defined as Scheduler
	// in proto/task_worker.proto. It has one field, a
	// *grpc.ClientConn which is passed in here during construction.
	schedClient := ga4gh_task_ref.NewSchedulerClient(conn)

	// We define fileClient to be any type that implements the
	// FileSystemAccess interface (2 functions, Get and Put)
	var fileClient tesTaskEngineWorker.FileSystemAccess

	// TODO: refactor to use case matching
	if *swiftDirArg != "" {
		storageDir, _ := filepath.Abs(*swiftDirArg)
		if _, err := os.Stat(storageDir); os.IsNotExist(err) {
			os.Mkdir(storageDir, 0700)
		}

		// Define fileClient to be type SwiftAccess
		fileClient = tesTaskEngineWorker.NewSwiftAccess()
	} else if *fileSystemArg != "" {
		o := []string{}
		for _, i := range strings.Split(*fileSystemArg, ",") {
			p, _ := filepath.Abs(i)
			o = append(o, p)
		}
		// Define fileClient to be type FileAccess
		fileClient = tesTaskEngineWorker.NewFileAccess(o)
	} else {
		storageDir, _ := filepath.Abs(*storageDirArg)
		if _, err := os.Stat(storageDir); os.IsNotExist(err) {
			os.Mkdir(storageDir, 0700)
		}
		// fileClient is now FileStorageAccess type
		fileClient = tesTaskEngineWorker.NewSharedFS(storageDir)
	}
	// fileMapper is a new FileMapper created using the above fileClient
	fileMapper := tesTaskEngineWorker.NewFileMapper(&schedClient, fileClient, volumeDir)

	// create 16-byte "random" ID
	u, _ := uuid.NewV4()
	// Create a ForkManager
	manager, _ := tesTaskEngineWorker.NewLocalManager(*nworker, u.String())
	if *timeoutArg <= 0 {
		// Is there a reason it isn't &schedClient??
		manager.Run(schedClient, *fileMapper)
	} else {
		var startCount int32
		lastPing := time.Now().Unix()
		// TODO: Fix data race. SetStatusCheck takes a func() which accesses lastPing. It gets assigned internally within manager to the checkFunc() field. This is used within the manager.Start() call a few lines later. manager.Start() spawns a new thread, from which checkFunc() is called. Notice that near the end of this file, lastPing is read. This means that one thread is reading lastPing while another is writing to lastPing concurrently. This results in a data race.
		manager.SetStatusCheck(func(status tesTaskEngineWorker.EngineStatus) {
			if status.JobCount > startCount || status.ActiveJobs > 0 {
				startCount = status.JobCount
				lastPing = time.Now().Unix()
			}
		})
		manager.Start(schedClient, *fileMapper)
		for time.Now().Unix()-lastPing < int64(*timeoutArg) {
			time.Sleep(5 * time.Second)
		}

	}
}
