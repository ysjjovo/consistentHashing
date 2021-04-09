package job

import (
	uuid "github.com/satori/go.uuid"
	"io/ioutil"
	"strings"
	"testing"
)

func TestNewSortedJob(t *testing.T) {
	sortedJobs := NewSortedJob()

	for _,v:=range sortedJobs.jobIdx{
		println(v)
	}
	println("jobs")
	for k,v:=range sortedJobs.jobs{
		println(k,v.Hash,v.Version,v.Cron)
	}
}

func newJob()SortedJobs{
	var sorted SortedJobs
	var jobs []*Job
	jobs=append(jobs,&Job{Hash:20,Id:"20"},&Job{Hash:10,Id:"10"})
	sorted.jobs=jobs

	sorted.jobIdx=append(sorted.jobIdx,1,0)
    return sorted
}
func TestSortedJobs_FindJobs(t *testing.T) {
	var sorted SortedJobs
	sorted = newJob()
	var jobs []*Job
	var size int

	jobs = sorted.FindJobs(0, 9)
	if size!=0{
		t.Fatalf("len should be zero")
	}
	//one
	jobs = sorted.FindJobs(0, 10)
	size=len(jobs)
	if size!=1{
		t.Fatalf("len should be one,%d",size)
	}
	//one
	jobs = sorted.FindJobs(10, 30)
	size=len(jobs)
	if size!=1{
		t.Fatalf("len should be one,%d",size)
	}
	if jobs[0].Hash != 20 {
		t.Fatalf("hash should be 20,%d",jobs[0].Hash)
	}
	//all
	jobs = sorted.FindJobs(0, 30)
	size=len(jobs)
	if size!=2{
		t.Fatalf("len should be 2,%d",size)
	}
	//all reverse
	jobs = sorted.FindJobs(30, 20)
	size = len(jobs)
	if size != 2{
		t.Fatalf("len should be 2,%d", size)
	}
}

func TestAddJob(t *testing.T){
	s := newJob()

	s.addJob(&Job{
		Id:      "100",
		Hash:    10,
	})
	s.addJob(&Job{
		Id:      "110",
		Hash:    11,
	})
	s.addJob(&Job{
		Id:      "300",
		Hash:    30,
	})
	for _,v:= range s.jobIdx{
		j:=s.jobs[v]
		println("v:",v,"jobId:",j.Id,"hash:",j.Hash)
	}
}

func TestDelJob(t *testing.T) {
	var s SortedJobs
	job := s.DelJob("notExists")
	if job!=nil{
		t.Fatalf("should be nil")
	}
	s.AddJob("1")//2212294583
	s.AddJob("2")//450215437
	s.AddJob("3")//1842515611
	s.AddJob("4")//4088798008
	//1 2 0 3
	print(&s)
	var deleted *Job
	deleted = s.DelJob("3")
	println("mid deleted ",deleted.Id)
	print(&s)

	deleted = s.AddJob("3")
	println("mid added",deleted.Id)
	print(&s)

	deleted = s.DelJob("2")
	println("left deleted ",deleted.Id)
	print(&s)
	//
	deleted = s.AddJob("2")
	println("left added",deleted.Id)
	print(&s)

	deleted = s.DelJob("4")
	println("right deleted ",deleted.Id)
	print(&s)

	deleted = s.AddJob("4")
	println("right added",deleted.Id)
	print(&s)
}

func print(s *SortedJobs){
	for _,v:= range s.jobIdx{
		j:=s.jobs[v]
		println("v:",v,"jobId:",j.Id,"hash:",j.Hash)
	}
}
const JobSize=1000

func TestGenJobIds(t *testing.T){
	sb:=&strings.Builder{}

	for i:=1;i<=JobSize;i++{
		u,_ := uuid.NewV4()
		jobId := u.String()
		sb.WriteString(jobId)
		sb.Write([]byte("\n"))
	}
	ioutil.WriteFile("job-ids.txt", []byte(sb.String()), 0644)
}