# WolfX

[![CI](https://github.com/yackrru/wolfx/actions/workflows/ci.yml/badge.svg)](https://github.com/yackrru/wolfx/actions/workflows/ci.yml)
[![CodeQL](https://github.com/yackrru/wolfx/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/yackrru/wolfx/actions/workflows/codeql-analysis.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/yackrru/wolfx.svg)](https://pkg.go.dev/github.com/yackrru/wolfx)

A batch framework for golang.

## Overview
### Architecture
There is Job as the top-level concept and WolfX instance bundles multiple jobs.  
Each Job has a unique job name and target job can be executed by passing the job name to WolfX instance.  
Each Job has one or more steps, which can be executed sequentially or concurrently.  
Each Step has one reader and one writer.  
The Reader and Writer runs on separate goroutines and they are connected by a channel.
![image](https://user-images.githubusercontent.com/50540555/154090445-92841692-c5b2-46b1-b2b2-6f503d96c25f.png)

## Getting started
### Installing
Install the library first.
```
go get github.com/yackrru/wolfx
```
### Usage
Note that the following sample code is incomplete.  
DBToFileJob just works as a Job and it has one step, ReadAndOutputStep, which is set to execute sequentially.  
The ReadAndOutputStep is incompletely configured,
but it can be read that the Reader is set to read data from the database and the Writer is set to write data to a file.  
Finally, you can execute this job by passing DBToFileJob as an argument to Execute.
```go
package main

import (
	"context"
	"github.com/yackrru/wolfx"
	"github.com/yackrru/wolfx/integration/database"
	"github.com/yackrru/wolfx/integration/file"
	"github.com/yackrru/wolfx/middleware"
)

func Execute(jobName string) int {
	wx := wolfx.New()

	wx.Add(new(DBToFileJob))

	if err := wx.Run(jobName); err != nil {
		middleware.Logger.Fatal(err)
	}
	return 0
}

type DBToFileJob struct {}

func (j *DBToFileJob) Name() string {
	return "DBToFileJob"
}

func (j *DBToFileJob) Run() error {
	return wolfx.NewJobBuilder().
		Single(j.ReadAndOutputStep).
		Build()
}

func (j *DBToFileJob) ReadAndOutputStep(ctx context.Context) error {
	return wolfx.NewStepBuilder(ctx).
		SetReader(database.NewReader(&database.ReaderConfig{})).
		SetWriter(file.NewWriter(&file.WriterConfig{})).
		Build()
}
```

## Built-in integrations
The following can be used as Reader or Writer in Step.

| Type   | Package.Name    | Description                                                                                              |
|:-------|:----------------|:---------------------------------------------------------------------------------------------------------|
| Reader | file.Reader     | Reads a file using the file.CSVReader interface, which is satisfied by the standard package csv/Reader.  |
| Writer | file.Writer     | Writes a file using the file.CSVWriter interface, which is satisfied by the standard package csv/Writer. |
| Reader | database.Reader | Use sql/DB to load data with cursor from database.                                                       |
| Writer | database.Writer | Use sql/DB to import data to database.                                                                   |

## Sample codes
Work in progress.
