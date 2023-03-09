#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {	//work需要知道的信息
		// Lab4: Your definition here.
		int tasktype;
        int index;
        int file_nums;
        string filename;
	};

	friend marshall &operator<<(marshall &m, const AskTaskResponse &res) {
        return m << res.tasktype << res.index << res.file_nums << res.filename;
    }

    friend unmarshall &operator>>(unmarshall &u, AskTaskResponse &res) {
        return u >> res.tasktype >> res.index >> res.file_nums >> res.filename;
    }

	struct AskTaskRequest {
		// Lab4: Your definition here.
	};

	struct SubmitTaskResponse {
		// Lab4: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab4: Your definition here.
	};

};

#endif

