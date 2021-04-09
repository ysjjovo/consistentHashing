package job

import (
	"consistentHashing/component"
	"consistentHashing/component/hashingCycle"
	uuid "github.com/satori/go.uuid"
	"io/ioutil"
	"sort"
	"strings"
	"sync"
)

type Job struct{
	Id      string
	Hash    int
	Cron    string
	Version string
}
var Wheel =&TimingWheel{jobs:make(map[string]*Job,16)}
var jobVerPrefix ="jobs.version"

type TimingWheel struct{
	Mu sync.RWMutex
	jobs map[string]*Job
}
func (t *TimingWheel)Add(j *Job){
	defer t.Mu.Unlock()
	t.Mu.Lock()
	t.jobs[j.Id]=j
}
func (t *TimingWheel)Jobs() map[string]*Job{
	return t.jobs
}
//开始调度时锁定
//todo 可重试更新version
func (j *Job) Schedule()(err error){
	if err = j.LockVersion(); err!= nil{
		return
	}
	Wheel.Add(j)//加入时间轮
	return
}

func (j *Job) LockVersion()(err error){
	u, _ := uuid.NewV4()
	ver := u.String()
	_, err = component.Etcd.Put(jobVerPrefix+"/"+j.Id, ver)
	if err == nil {
		j.Version = ver
	}
	return
}
type SortedJobs struct{
	jobIdx []int
	jobs []*Job
	etcdPrefix string
}

func NewSortedJob()*SortedJobs{
	sorted:=SortedJobs{}
	sorted.loadJobs()
	var idxs []int
	for i:=0;i<len(sorted.jobs);i++{
		idxs=append(idxs,i)
	}
	sorted.jobIdx=idxs
	sorted.Sort()
	return &sorted
}
//todo load from db
func (s *SortedJobs)loadJobs(){
	var jobs []*Job
	str,_ :=ioutil.ReadFile("../job/job-ids.txt")

	split := strings.Split(string(str), "\n")
	for i:=0;i<len(split)-1;i++{
		jobs=append(jobs,s.getJobFromDb(split[i]))
	}
	s.jobs=jobs
}
//查找(start,end]的job
//start<end   (start,inf),[0,end)
//start>end
func (s *SortedJobs)FindJobs(hashStart,hashEnd int)[]*Job{
	if len(s.jobs) == 0 {
		return nil
	}
	if hashStart < hashEnd {
		return s.findBetween(hashStart,hashEnd)
	}
	return append(s.findBetween(hashStart,int(^uint(0) >> 1)),s.findBetween(-1,hashEnd)...)
}


func (s *SortedJobs)AddJob(id string)(job *Job){
	//todo get from db
	job = s.getJobFromDb(id)
	s.addJob(job)
	return job
}
func (s *SortedJobs)addJob(job *Job) {
	hash:=job.Hash
	size := len(s.jobIdx)
	if size == 0 {
		s.jobIdx=append([]int{},0)
		s.jobs=append([]*Job{},job)
		return
	}
	i :=s.next(hash,0, size-1)
	//判断是否已经添加
	for j:= i-1;j>-1;j--{
		idx:=s.jobIdx[j]
		jb := s.jobs[idx]
		if hash != jb.Hash {
			break;
		}
		if job.Id == jb.Id {
			return
		}
	}

	tmp:=append([]int{},s.jobIdx[i:]...)
	s.jobIdx=append(append(s.jobIdx[:i], size),tmp...)
	s.jobs=append(s.jobs,job)
}
func (s *SortedJobs)DelJob(jobId string)(job *Job) {
	i,job:=s.Find(jobId)
	if i == -1 {//不存在
		return
	}

	idx:=s.jobIdx[i]
	job=s.jobs[idx]
	s.jobs=append(s.jobs[:idx],s.jobs[idx+1:]...)
	s.jobIdx=append(s.jobIdx[:i],s.jobIdx[i+1:]...)
	for j:=0;j<len(s.jobs);j++{//索引往前移
	    if s.jobIdx[j]> idx{
			s.jobIdx[j]--
		}
	}
	return
}

func (s *SortedJobs) getJobFromDb(jobId string)(job *Job){
	//todo get from db
	return &Job{Id: jobId,Hash:hashingCycle.Hash(jobId),Cron:"0/1 * * * *"}
}


func (s *SortedJobs)findBetween(hashStart,hashEnd int)(jobs []*Job) {
	size := len(s.jobIdx)
	i:=s.next(hashStart,0, size-1)
	for ;i<size;i++{
		idx:=s.jobIdx[i]
		job := s.jobs[idx]
		if job.Hash>hashEnd{
			break
		}
		jobs=append(jobs,job)
	}
	return
}
func (s *SortedJobs) Find(id string)(i int, job*Job){
	hash:=hashingCycle.Hash(id)

	size := len(s.jobs)
	if size == 0 {
		return -1,nil
	}

	i=s.next(hash,0, size-1)-1

	//hash值相同碰撞,判断id是否匹配
	exists:=false
	for ;i>-1;i--{
		idx:=s.jobIdx[i]
		jb := s.jobs[idx]
		if hash != jb.Hash {
			break;
		}
		if id == jb.Id {
			exists=true
			break;//找到
		}
	}
	if exists {
		return i,s.jobs[s.jobIdx[i]]
	}
	return -1,nil
}

//找刚好大于hash的索引
func (s *SortedJobs) next(hash,i,j int)int{
	//println("hash,i,j",hash,i,j)
	if i == j {
		midVal :=s.jobs[s.jobIdx[i]].Hash
		if midVal > hash {
			return i
		} else {
			return i + 1
		}
	}
	mid:=(i+j)/2
	midVal :=s.jobs[s.jobIdx[mid]].Hash
	if midVal == hash {
		return mid + 1
	} else if midVal > hash {
		if mid == i {
			return mid
		}
		return s.next(hash,i,mid-1)
	}
	if mid == j {
		return mid + 1
	}
	return  s.next(hash,mid+1,j)
}


//排序
func (s *SortedJobs) Sort() {
	sort.Sort(s)
}
func (s *SortedJobs) Len() int {
	return len(s.jobs)
}

func (s *SortedJobs) Swap(i, j int) {
	s.jobIdx[i], s.jobIdx[j] = s.jobIdx[j],s.jobIdx[i]
}

func (s *SortedJobs) Less(i, j int) bool {
	return s.jobs[s.jobIdx[i]].Hash < s.jobs[s.jobIdx[j]].Hash
}
