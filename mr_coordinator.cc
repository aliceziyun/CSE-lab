#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string>
#include <vector>
#include <mutex>

#include "mr_protocol.h"
#include "rpc.h"

using namespace std;

struct Task
{
	int taskType;			  // should be either Mapper or Reducer
	bool isAssigned = false;  // has been assigned to a worker
	bool isCompleted = false; // has been finished by a worker
	int index;				  // index to the file
};

class Coordinator
{
public:
	Coordinator(const vector<string> &files, int nReduce);
	mr_protocol::status askTask(int, mr_protocol::AskTaskResponse &reply);
	mr_protocol::status submitTask(int taskType, int index, bool &success);
	bool isFinishedMap();
	bool isFinishedReduce();
	bool Done();

private:
	vector<string> files;	  // 文件们
	vector<Task> mapTasks;	  // 分发下去的mapTask
	vector<Task> reduceTasks; // 分发下去的reduceTask

	mutex mtx;

	long completedMapCount;	   // 做完的map任务数量
	long completedReduceCount; // 做完的reduce任务数量
	bool isFinished;

	string getFile(int index); // 根据index返回对应文件
};

// Your code here -- RPC handlers for the worker to call.

mr_protocol::status Coordinator::askTask(int, mr_protocol::AskTaskResponse &reply)
{
	// Lab4 : Your code goes here.
	//	尝试分配任务
	this->mtx.lock();
	int id = -1;
	cout<<"in the ask Task"<<endl;
	if (this->completedMapCount < long(this->mapTasks.size()))	//一定得把map做完再做reduce
	{ // 先尝试分配mapTask
		for (int i = 0; i < (int)this->mapTasks.size(); ++i)
		{ // 遍历任务集，找一个没做过的任务
			cout<<mapTasks[i].isCompleted<<" "<<mapTasks[i].isAssigned<<endl;
			if (!mapTasks[i].isCompleted && !mapTasks[i].isAssigned)
			{
				id = i;
				mapTasks[i].isAssigned = true;
				reply.index = mapTasks[id].index;
				reply.tasktype = MAP;
				break;
			}
		}
	}
	else if (this->completedReduceCount < long(this->reduceTasks.size()))
	{ // 再尝试分配reduceTask
		for (int i = 0; i < (int)this->reduceTasks.size(); ++i)
		{
			if (!reduceTasks[i].isCompleted && !reduceTasks[i].isAssigned)
			{
				id = i;
				reduceTasks[i].isAssigned = true;
				reply.index = reduceTasks[id].index;
				reply.tasktype = REDUCE;
				break;
			}
		}
	}
	
	if(id == -1){
		reply.index = -1;
		reply.tasktype = NONE;
		reply.filename = "";
		this->mtx.unlock();
		return mr_protocol::OK;
	}
	this->mtx.unlock();
	reply.filename = getFile(reply.index);
	reply.file_nums = files.size();
	// cout << "assgin task with id" << reply.index << endl;
	return mr_protocol::OK;
}

mr_protocol::status Coordinator::submitTask(int taskType, int index, bool &success)
{
	// Lab4 : Your code goes here.
	this->mtx.lock();
	switch (taskType)
	{
	case MAP:
		cout<<"map task"<<index<<"ok"<<endl;
		mapTasks[index].isCompleted = true;
		mapTasks[index].isAssigned = false;
		this->completedMapCount++;
		break;
	case REDUCE:
		reduceTasks[index].isCompleted = true;
		reduceTasks[index].isAssigned = false;
		this->completedReduceCount++;
		break;
	default:
		break;
	}
	if (this->completedMapCount >= (long)mapTasks.size() && this->completedReduceCount >= (long)reduceTasks.size())
		this->isFinished = true;
	this->mtx.unlock();
	success = true;
	// cout<<"submit index"<<index<<"ok"<<endl;
	return mr_protocol::OK;
}

string Coordinator::getFile(int index)
{
	this->mtx.lock();
	string file = this->files[index];
	this->mtx.unlock();
	return file;
}

bool Coordinator::isFinishedMap()
{
	bool isFinished = false;
	this->mtx.lock();
	if (this->completedMapCount >= long(this->mapTasks.size()))
	{
		isFinished = true;
	}
	this->mtx.unlock();
	return isFinished;
}

bool Coordinator::isFinishedReduce()
{
	bool isFinished = false;
	this->mtx.lock();
	if (this->completedReduceCount >= long(this->reduceTasks.size()))
	{
		isFinished = true;
	}
	this->mtx.unlock();
	return isFinished;
}

//
// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
//
bool Coordinator::Done()
{
	bool r = false;
	this->mtx.lock();
	r = this->isFinished;
	this->mtx.unlock();
	return r;
}

//
// create a Coordinator.
// nReduce is the number of reduce tasks to use.
//
Coordinator::Coordinator(const vector<string> &files, int nReduce)
{
	this->files = files;
	this->isFinished = false;
	this->completedMapCount = 0;
	this->completedReduceCount = 0;

	int filesize = files.size();
	for (int i = 0; i < filesize; i++)
	{
		this->mapTasks.push_back(Task{mr_tasktype::MAP, false, false, i});
	}
	for (int i = 0; i < nReduce; i++)
	{
		this->reduceTasks.push_back(Task{mr_tasktype::REDUCE, false, false, i});
	}
}

int main(int argc, char *argv[])
{
	int count = 0;

	if (argc < 3)
	{
		fprintf(stderr, "Usage: %s <port-listen> <inputfiles>...\n", argv[0]);
		exit(1);
	}
	char *port_listen = argv[1];

	setvbuf(stdout, NULL, _IONBF, 0);

	char *count_env = getenv("RPC_COUNT");
	if (count_env != NULL)
	{
		count = atoi(count_env);
	}

	vector<string> files;
	char **p = &argv[2];
	while (*p)
	{
		files.push_back(string(*p));
		++p;
	}

	rpcs server(atoi(port_listen), count);

	Coordinator c(files, REDUCER_COUNT);

	//
	// Lab4: Your code here.
	// Hints: Register "askTask" and "submitTask" as RPC handlers here
	//
	server.reg(mr_protocol::asktask, &c, &Coordinator::askTask);
	server.reg(mr_protocol::submittask, &c, &Coordinator::submitTask);

	while (!c.Done())
	{
		sleep(1);
	}

	return 0;
}
