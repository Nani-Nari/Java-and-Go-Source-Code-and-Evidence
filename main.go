package main

import (
 "encoding/csv"
 "fmt"
 "log"
 "os"
 "sort"
 "strconv"
 "sync"
 "time"
)
const taskCount=12
const workerCount=4
type Result struct { ID int; Square int; Worker string }
type TaskQueue struct { tasks chan int }
func NewTaskQueue(capacity int) *TaskQueue { return &TaskQueue{tasks:make(chan int,capacity)} }
func (q *TaskQueue) addTask(id int) { q.tasks <- id }
func (q *TaskQueue) getTask() (int,bool) { id,ok:= <-q.tasks; return id,ok }
func worker(id int,q *TaskQueue,results chan<- Result,wg *sync.WaitGroup) {
 defer wg.Done()
 name:=fmt.Sprintf("Worker-%d",id)
 log.Printf("%s started",name)
 defer log.Printf("%s completed",name)
 for { task,ok:=q.getTask(); if !ok { return }
  time.Sleep(20*time.Millisecond)
  results <- Result{task,task*task,name}
  log.Printf("%s processed task %d",name,task)
 }
}
func saveResults(path string,results []Result) (err error) {
 file,err:=os.Create(path); if err!=nil { return fmt.Errorf("create results: %w",err) }
 defer func(){ if closeErr:=file.Close(); err==nil && closeErr!=nil {err=closeErr} }()
 writer:=csv.NewWriter(file)
 if err=writer.Write([]string{"task_id","square","worker"});err!=nil{return err}
 for _,r:=range results { if err=writer.Write([]string{strconv.Itoa(r.ID),strconv.Itoa(r.Square),r.Worker});err!=nil{return err} }
 writer.Flush()
 if err=writer.Error();err!=nil{return fmt.Errorf("write results: %w",err)}
 return nil
}
func run() error {
 q:=NewTaskQueue(taskCount)
 resultsCh:=make(chan Result,taskCount)
 var wg sync.WaitGroup
 for i:=1;i<=workerCount;i++ {wg.Add(1);go worker(i,q,resultsCh,&wg)}
 for i:=1;i<=taskCount;i++ {q.addTask(i)}
 close(q.tasks)
 go func(){wg.Wait();close(resultsCh)}()
 results:=make([]Result,0,taskCount)
 for r:=range resultsCh {results=append(results,r)}
 sort.Slice(results,func(i,j int)bool{return results[i].ID<results[j].ID})
 if len(results)!=taskCount{return fmt.Errorf("expected %d results, got %d",taskCount,len(results))}
 seen:=make(map[int]bool)
 for _,r:=range results {if seen[r.ID]||r.Square!=r.ID*r.ID{return fmt.Errorf("duplicate or invalid task %d",r.ID)};seen[r.ID]=true}
 if err:=saveResults("go_results.csv",results);err!=nil{return err}
 fmt.Printf("GO SUCCESS: %d unique tasks processed; saved go_results.csv\n",len(results))
 fmt.Println("Task 1 -> 1; Task 12 -> 144")
 return nil
}
func main(){if err:=run();err!=nil{log.Printf("ERROR: %v",err);os.Exit(1)}}
