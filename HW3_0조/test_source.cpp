#include "../Common/mdp.h"
#include "../Common/mdp_client.h"
#include "../Common/monitoring.h"
#include "../Common/basexml10.h"
#include "../Common/utility.h"
#include "../Common/mdp_worker.h"
#include "../Common/hash_lib.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <string.h>
#include <stdint.h>
#include <sstream>
#include <vector>
#include <iostream>
#include <sys/stat.h>
#include <iconv.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>


#define STREAM_PORT_PUB		6015
//#define STREAM_PORT_XPUB	6016
#define STREAM_PORT_PULL	6017
#define DEBUG_MODE	1
#define HWM_SIZE	1000

#define TURE	1
#define FALSE	0
#define ECG 0x21
#define PPG 0x22
#define ACC 0x23
#define BREATH 0x24


using namespace std;

/*
struct th_args
{
	int heartbeat;
	mdp_worker_t *se;
};
*/
//table & delete count 
int count = 0;
zhash_t *z_hash = zhash_new();

zlist_t *z_list = zlist_new();

int config_flag = 0;

/*
<진행상황>
1. cradle과 front connection ok and data receive ok
2. received data -> RFProxy ok
3. receive RFProxy -> send to Stream Agent
4. XML format ok
5. xml 확인
<해야할것>

2. pub/sub data send and 전송률 및 reliable
3. 다중 connection


////////////////////////////////////////////////////////////////////
Post-it -> request postit , ack postit

1. request post-it
	const char* demoStart =
		"<?xml version=\"1.0\" encoding=\"euc-kr\"?>"
		"<postit version=\"1.0\" id=\"3777492913EC9BB6\" name=\"STREAMING_AGENT\" "
				"datetime=\"2016-06-10 13:06:10\" kind=\"Person\" "
				"type=\"Text\" priority=\"Normal\" callback=\"No\" "
				"stream=\"No\" sequence=\"1\" ttl=\"24\">"
			"<subject>STREAMING_REQUEST</subject>"
			"<sources>"
				"<address type=\"User\" device=\"Phone\">ip_addr</address>"
			"</sources>"
			"<destinations>"
				"<address type=\"Router\" device=\"SoSpDevice\" Agent_opt=\"STREAMING_AGENT\">155.230.186.215</address>"
			"</destinations>"
			"<content>"
				"<DeviceId>D400AA58</DeviceId>"	//device id..
				"<Command>ServiceReuqest</Command>"	//ServiceRequest, ServiceStop, SavedDataListRequest, SavedDataRequest
				"<Select>A001FF</Select>"	//ppg,ecg...select..
				"<Identity>A</Identity>	//sub identity..
			"</content>"
		"</postit>";

2. ack post-it

	const char* demoStart =
	"<?xml version=\"1.0\" encoding=\"euc-kr\"?>"
	"<postit version=\"1.0\" id=\"3777492913EC9BB6\" name=\"STREAMING_AGENT\" "
			"datetime=\"2016-06-10 13:06:10\" kind=\"Person\" "
			"type=\"Text\" priority=\"Normal\" callback=\"No\" "
			"stream=\"No\" sequence=\"1\" ttl=\"24\">"
		"<subject>STREAMING_ACK</subject>"
		"<sources>"
			"<address type=\"User\" device=\"Router\">ip_addr</address>"
		"</sources>"
		"<destinations>"
			"<address type=\"phone\"155.230.186.215</address>"
		"</destinations>"
		"<content>"
			"</Sampling>"Meta information insert.."</Sampling>"	//meta info(sampling rate..)
			"</TimeLocation>"time and location information insert.."</TimeLocation>"
		"</content>"
	"</postit>";


*/

int reverse_dev_id(int PaarID)
{
	int ret_ID = PaarID;
	int first = (PaarID << 16) & 0xFF000000;
	int second = (PaarID)& 0x00FF0000;
	int third = (PaarID >> 16) & 0x0000FF00;
	int fourth = (PaarID)& 0x000000FF;
	return ret_ID = first | second | third | fourth;
}
static bool send_activity_prescription(string prescription_xml)
{
	int verbose = 0;
	mdp_client_t* session = mdp_client_new(LOCAL_BROKER_ADDR, verbose);
	int timeout = 250;
	mdp_client_setsockopt(session, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
	zclock_sleep(150);
	zmsg_t *send_msg = zmsg_new();
	zmsg_addstr(send_msg, prescription_xml.c_str());
	mdp_client_send(session, POSTIT_AGENT, &send_msg);
	zclock_log("send prescription to RF Proxy");
	zmsg_t* return_msg = mdp_client_recv(session, NULL, NULL);
	if (return_msg)
	{
		zclock_log("I : streaming_agent received reply of prescription");
		zmsg_dump(return_msg);
	}
	return streq(zmsg_popstr(return_msg), "200");
	mdp_client_destroy(&session);
	zmsg_destroy(&return_msg);
	zclock_sleep(500);
}

// @brief stream request postit create
// @return postit
static string make_activity_prescription(
	string destination,		///< streaming할 Device ID
	string data)			///< 설정 Data
{
	//rand for callback id
	int msg_ID;
	srand((unsigned)time(NULL));
	msg_ID = rand();

	//set postit header
	stringstream prescription_postit;
	prescription_postit.str("");
	prescription_postit << "<?xml version=\"1.0\" encoding=\"euc-kr\"?>";
	prescription_postit << "<postit version=\"1.0\" id=\"" << msg_ID << "\"  name=\"Activity Result\"" << " callback=\"No\"" << " datetime=\"" << getTime() << "\" type=\"Text\">";
	prescription_postit << "<subject>Activity_Result</subject>";

	//set source address
	prescription_postit << "<sources>";
	// make address attribute from get SlimHub IP address
	prescription_postit << "<address type=\"Router\" Agent_opt=\"STREAMING_AGENT\">" << getLocalIPAddress().data() << "</address>";
	prescription_postit << "</sources>";

	//set destination address
	prescription_postit << "<destinations>";
	//Destination address : PAAR Watch ID
	prescription_postit << "<address type=\"User\" device=\"SoSpDevice\">" << destination << "</address>";
	prescription_postit << "</destinations>";

	//set result data
	prescription_postit << "<content>";
	prescription_postit << "<binary>";
	prescription_postit << data;
	prescription_postit << "</binary>";
	prescription_postit << "</content>";
	prescription_postit << "</postit>";

	//	printf("\n\n%s\n\n", prescription_postit.str().c_str());
	//    string prescription = prescription_postit.str();
	return prescription_postit.str();
	//	return prescription;
}

static bool ServiceReqMessage(
	const int mobile_id		////< 기기 ID
	)
{
	int verbose = 0;
	char str_mobileid[20];

	mdp_client_t* session = mdp_client_new(LOCAL_BROKER_ADDR, verbose);

	zmsg_t *send_msg = zmsg_new();
	zmsg_addstr(send_msg, "ReqMessage");
	sprintf(str_mobileid, "%x", mobile_id);
	zmsg_addstr(send_msg, str_mobileid);
	zmsg_addstr(send_msg, STREAMING_AGENT);
	mdp_client_send(session, RFPROXY_AGENT, &send_msg);

	zmsg_t* return_msg = mdp_client_recv(session, NULL, NULL);
	mdp_client_destroy(&session);

	return streq(zmsg_popstr(return_msg), "200");
}

unsigned int set_bit(unsigned int value)
{
	value = value & 0x00FF;
	return value;
}

//just for test print...
void print_ecg_data(string recv_data, unsigned int d_len)
{
	unsigned int ecg_data[16] = { 0, };

	printf("ecg data print : ");

	for (int i = 0; i < d_len; i++)
	{
		ecg_data[i] = recv_data.data()[i + 6];
		ecg_data[i] = set_bit(ecg_data[i]);
		printf("data[%d]:%02X", i + 1, ecg_data[i]);
	}
	printf("\n");
}
void print_ppg_data(string recv_data, unsigned int d_len)
{
	unsigned int ppg_data[16] = { 0, };

	printf("ppg data print : ");

	for (int i = 0; i < d_len; i++)
	{
		ppg_data[i] = recv_data.data()[i + 6];
		set_bit(ppg_data[i]);
		printf("data[%d]:%2X", i + 1, ppg_data[i]);
	}
	printf("\n");
}
void print_acc_data(string recv_data, unsigned int d_len)
{
	unsigned int acc_data[16] = { 0, };

	printf("acc data print : ");

	for (int i = 0; i < d_len; i++)
	{
		acc_data[i] = recv_data.data()[i + 6];
		set_bit(acc_data[i]);
		printf("data[%d]:%02X", i + 1, acc_data[i]);
	}
	printf("\n");
}
void print_breath_data(string recv_data, unsigned int d_len)
{
	unsigned int breath_data[16] = { 0, };

	printf("breath data print : ");

	for (int i = 0; i < d_len; i++)
	{
		breath_data[i] = recv_data.data()[i + 6];
		set_bit(breath_data[i]);
		printf("data[%d]:%2X", i + 1, breath_data[i]);
	}
	printf("\n");
}
/// @brief XML 형태로 되어있는 포스트잇 메시지를 파싱하여 필요한 정보를 획득
/// @return 획득하고자 하는 정보
static char * get_xml_information(
	char *xml,			///< 포스트잇 메시지
	const char *xpath,	///< xpath
	const char *attr,	///< 찾고자 하는 어트리뷰트 값
	int verbose)
{
	char *ret_val = NULL;
	mdp_client_t *session = mdp_client_new(LOCAL_BROKER_ADDR, verbose);

	zmsg_t *request, *reply;
	request = zmsg_new();
	zmsg_addmem(request, strdup(xml), strlen(xml));
	zmsg_addstr(request, strdup(xpath));
	zmsg_addstr(request, strdup(attr));
	mdp_client_send(session, XMLPARSER_AGENT, &request);
	reply = mdp_client_recv(session, NULL, NULL);

	if (reply)
	{
		if (verbose)
		{
			zclock_log("I: PostItAgent receives reply of request:");
			zmsg_dump(reply);
		}

		char *code = zmsg_popstr(reply);
		char *val = zmsg_popstr(reply);

		if (streq(code, "200"))
			ret_val = strdup(val);
		else
			zclock_log("I: PostItAgent receives error reply from XMLParserAgent: %s", val);

		if (code) free(code);
		if (val) free(val);
		zmsg_destroy(&reply);
	}

	mdp_client_destroy(&session);

	return ret_val;
}
string correct_deviceid(char *orginal)
{
	char *dump = orginal;
	char temp[9] = { 0, };

	temp[0] = dump[4];
	temp[1] = dump[5];
	temp[2] = dump[2];
	temp[3] = dump[3];
	temp[4] = dump[0];
	temp[5] = dump[1];
	temp[6] = dump[6];
	temp[7] = dump[7];
	temp[8] = 0;

	string temp_str = temp;
	return temp_str;
}
void send_stream_stop(char *dev_id)
{
	byte datapacket[7] = { 0, };
	int idx = 0;
	datapacket[idx++] = (byte)0x86;	//CMD
	datapacket[idx++] = (byte)0xAA;	//service_id
	datapacket[idx++] = (byte)0x11;	//sequence number
	datapacket[idx++] = (byte)0x03; //datalen
	datapacket[idx++] = (byte)0xB0;	//data, type
	datapacket[idx++] = (byte)0x01; //data, len
	datapacket[idx++] = (byte)0x01; //data, start request

									//string temp_id = "aa00d358";
	string temp_id = correct_deviceid(dev_id);
	string prcdata = array_to_hex_string((unsigned char *)datapacket, sizeof(datapacket));
	string prc = make_activity_prescription(temp_id, prcdata);
#if defined(DEBUG_MODE)
	printf("make_activity_prescription() : %s \n", prc.data());
#endif					

	if (prc.compare("") == 0) {
	}
	else {
		zclock_sleep(100);
		bool is_ok = send_activity_prescription(prc);
		if (is_ok == TURE) {
			printf("send_stop_activity_prescription() success!\n");
		}
		else
			zclock_log("I[Straming agent] : streaming stop command tranfer FAIL!");
	}
}

void send_stream_request(char *dev_id)
{
	//for test
	//char *parr_id = "D400AA58";
	//int paarid = 3556811864;
	unsigned int device_info = strtoul(dev_id, NULL, 16);	//hex string -> int로(원래는 long type)
#if defined(DEBUG_MODE)
	printf("------------------------------------\n");
	printf("Test print for device id integer..\n");
	printf("device_id(int value) : %u \n", device_info);
#endif	

	//reverse paarid!(for test)
	//const int PaarID = 2852181080;
	unsigned int PaarID = reverse_dev_id(device_info);
#if defined(DEBUG_MODE)
	printf("reverse id(int value) : %u \n", PaarID);
	printf("------------------------------------\n");
#endif	
	//Service request
	bool request_ok = ServiceReqMessage(PaarID);
	if (request_ok == TURE)
		printf("ServiceReqMessage() : Stream service request to band via RFProxy ok!\n");
	else if (request_ok == FALSE)
		printf("ServiceReqMessage() : Stream service request to band via RFProxy fail!\n");

	//request command input
	byte datapacket[7] = { 0, };
	int idx = 0;
	datapacket[idx++] = (byte)0x86;	//CMD
	datapacket[idx++] = (byte)0xAA;	//service_id
	datapacket[idx++] = (byte)0x11;	//sequence number
	datapacket[idx++] = (byte)0x03; //datalen
	datapacket[idx++] = (byte)0xB0;	//data, type
	datapacket[idx++] = (byte)0x01; //data, len
	datapacket[idx++] = (byte)0x00; //data, start request

	string temp_id = correct_deviceid(dev_id);
	string prcdata = array_to_hex_string((unsigned char *)datapacket, sizeof(datapacket));
	//string prc = make_activity_prescription(device_id, prcdata);	
	string prc = make_activity_prescription(temp_id, prcdata);

#if defined(DEBUG_MODE)
	printf("------------------------------------\n");
	printf("Test print for activity_prescription...\n");
	printf("make_activity_prescription() : %s \n", prc.data());
#endif					

	if (prc.compare("") == 0) {
	}
	else {
		zclock_sleep(100);
		bool is_ok = send_activity_prescription(prc);
		if (is_ok == TURE) {
			zclock_log("I[Streaming_agent] : Stream request command transmit success");
		}
		else
			zclock_log("I[Straming agent] : streaming request command tranfer FAIL!");
	}
}
/*
//1. table을 10sec당 한번씩 확인하면서 현재 시간과 비교하여 10sec이상 차이나면 table에서 삭제 & count -= 1
//2. sub의 update를 1초에 한번 받아서, 해당 key(identity)에 update해준다.
//3. xsub -> xpub로 단순 데이터 보냄. xpub에서는 sub가 붙었는지 알 수 있다.
void timeout(int sig)
{
	//signal(SIGALRM, timeout);
		printf("10sec..!\n");
		//alarm(10);
		//key list를 받아온다.
		z_list = zhash_keys(z_hash);
		//현재 time
		int64_t time = zclock_time();

		//list의 첫 key값 받아온다.
		char *first_key = (char*)zlist_first(z_list);

		//모든 key(identity)들의  time과 현재  time 비교하여 10초이상 차이나면 delete한고 count -= 1 한다
		if (first_key == NULL) {
			printf("I[TIMER..] No sub connection(First key is null) ! \n");
		}
		else
		{
			int64_t *f_time = (int64_t *)zhash_lookup(z_hash, first_key);
			if (time - (*f_time) > MAX_DELAY)
			{
				printf("I[TIMER..] : Identity : %s :: timeout!! so delete \t", first_key);
				zhash_delete(z_hash, first_key);
				count -= 1;
				printf("so count -1 ! \n");
			}
			char *temp;
			while ((temp = (char *)zlist_next(z_list)) != NULL)
			{
				int64_t *rtime = (int64_t *)zhash_lookup(z_hash, temp);
				if (time - (*rtime) > MAX_DELAY) {
					printf("I[TIMER..] : Ientity : %s :: timeout!! so delete!!\t", temp);
					zhash_delete(z_hash, temp);
					count -= 1;
					printf("so count -1 ! \n");
				}
			}
		}

		if (count == 0)
		{
			printf("I[10sec] loop : sub is no connect..so die\n");
		}



}

*
static void proxy_thread(void *args, zctx_t *ctx, void *pipe)
{
	int verbose = 1;
	int heartbeat = *(int*)args;

	//XSUB/XPUB PROXY
	void *frontend = zsocket_new(ctx, ZMQ_SUB);
	int rc = zsocket_connect(frontend, "tcp://*:%d", STREAM_PORT_PUB);
	if (rc != 0)
	{
		printf("xsub connectg ok!()\n");
	}
	else
		printf("xsub connect fail..\n");

	zsocket_set_subscribe(frontend, "");

	void *backend = zsocket_new(ctx, ZMQ_XPUB);
	rc = zsocket_bind(backend, "tcp://155.230.15.45:%d", STREAM_PORT_XPUB);
	if (rc != 0)
	{
		printf("xpub bind ok!()\n");
	}
	else
		printf("xpub bind fail..\n");

	void *collector = zsocket_new(ctx, ZMQ_PULL);
	rc = zsocket_bind(collector, "tcp://155.230.15.45:%d", STREAM_PORT_PULL);
	if (rc != 0)
	{
		printf("collector bind ok!()\n");
	}
	else
		printf("collector bind fail..\n");

	while (true)
	{
		zmq_pollitem_t items[] = {
			{frontend, 0, ZMQ_POLLIN, 0},
			{backend, 0, ZMQ_POLLIN, 0},
			{collector, 0, ZMQ_POLLIN, 0}
		};

		rc = zmq_poll(items, 3, heartbeat*ZMQ_POLL_MSEC);
		if (rc == -1)
			break;

		if (items[0].revents & ZMQ_POLLIN)
		{
			char *msg = zstr_recv(frontend);
			if (!msg)
				break;

			zclock_log("I(XPUB SUB PROXY) : Recived message from PUB");
			zstr_send(backend, msg);

#if defined(DEBUG_MODE)
			printf("------------------------------------\n");
			printf("test print for xsub to xpub..\n");
			printf("data : %s \n", msg);
			printf("------------------------------------\n");
#endif
			free(msg);
		}
		if (items[1].revents & ZMQ_POLLIN)
		{
			//zframe_t *frame = zframe_recv(backend);
			char *msg = zstr_recv(backend);
			if (!msg)
				break;
			//byte *event = zframe_data(frame);
			//if (event[0] == 1) {
				//printf("connect sub..\n");
			//}
			zstr_send(backend, msg);
#if defined(DEBUG_MODE)
			printf("------------------------------------\n");
			printf("test print for xpub to sub..\n");
			printf("data : %s \n", msg);
			printf("------------------------------------\n");
#endif
			free(msg);
		}
		if (items[2].revents & ZMQ_POLLIN)
		{
			int64_t r_time = zclock_time();

			char *iden = zstr_recv(collector);
			//key list를 받아온다.
			z_list = zhash_keys(z_hash);
			//list의 첫 key값 받아온다.
			char *first_key = (char*)zlist_first(z_list);
			if (first_key == NULL) {
				printf("I[collector] : No List ... ! \n");
			}
			else
			{
				if (streq(first_key, iden))
					zhash_update(z_hash, first_key, (int64_t *)&r_time);

				char *temp;
				while ((temp = (char *)zlist_next(z_list)) != NULL)
				{
					if (streq(temp, iden))
						zhash_update(z_hash, temp, (int64_t *)&r_time);
				}

			}
		}
	}

	if (zctx_interrupted)
		printf("W: interrupt received, killing proxy thread...\n");

}
*/
static void streaming_agent(void *args, zctx_t *ctx, void *pipe)
{
	int verbose = 0;
	int heartbeat = *(int*)args;
	int hwm = HWM_SIZE;
	int i = 1;
	//현재 파일 저장 형태 잘 모르므로 간단히 그냥 저장만함.
	FILE *fd;
	fd = fopen("/home/bio_file_dir/test1.txt", "w");
	if (fd != NULL)
	{
		printf("open file ok,,,\n");
	}

	//pub sub(expresso pattern)
	//LVC or Clone Pattern 쓸 경우 -> PROXY 부분 수정필요
	void *publisher = zsocket_new(ctx, ZMQ_PUB);
	int rc = zsocket_bind(publisher, "tcp://155.230.15.45:%d", STREAM_PORT_PUB);
	if (rc != 0)
	{
		zclock_log("pub bind() ok!");
	}
	zsocket_set_hwm(publisher, hwm);


	while (true)
	{
		zmq_pollitem_t items[] = {
			{ pipe,  0, ZMQ_POLLIN, 0 } };

		rc = zmq_poll(items, 1, heartbeat * ZMQ_POLL_MSEC);
		if (rc == -1)
			break;              //  Interrupted

		// 부모로부터 파이프로 메시지를 받았다면...
		if (items[0].revents & ZMQ_POLLIN)
		{
			zmsg_t *msg = zmsg_recv(pipe);
			if (!msg)
				break;          //  Interrupted

			if (verbose)
			{
				zclock_log("I: received message from Proxy..");
				zmsg_dump(msg);
			}

			char *MID = zmsg_popstr(msg);
			char *content = zmsg_popstr(msg);
			/*
			#if defined(DEBUG_MODE)
						printf("recv data : %s \n", content);
			#endif
			*/
			//write file
			char add_str[20];
			sprintf(add_str, "seq : %d", i++);
			int add_str_size = sizeof(add_str);


			fprintf(fd, "%s\n", content);
			//pub, sub test는 차후에


			char temp_val[100];
			sprintf(temp_val, "%s", content);

			strncat(temp_val, add_str, add_str_size);


			//	printf("I[Streaming Agent] recv data from proxy agent : %s \n", temp_val);
			if (zstr_send(publisher, temp_val) == -1)
				break;

			//			char st[10];
			//			sprintf(st, "%c-%03d", 'A', randof(1000));
			//			if(zstr_send(publisher, st) == -1)
			//			    break;

			string temppacket = content;
			string contentpacket = hex_string_to_array(temppacket);

			unsigned int S_id = contentpacket.data()[0];
			unsigned int Seq_num = contentpacket.data()[1];
			unsigned int len = contentpacket.data()[2];
			unsigned int type = contentpacket.data()[3];
			unsigned int d_len = contentpacket.data()[4];
			unsigned int service_id = contentpacket.data()[5];
			service_id = set_bit(service_id);


			//fprintf(fd, "%s\n", w_data);


			//향후 Select 된 거에 따라 나누어서 보내주어야함..

			/*
//data receive test
#if defined(DEBUG_MODE)
			//service id => 21-ecg, 22-ppg, 23-acc, 24-breathe
			switch (service_id)
			{
			case ECG:
				print_ecg_data(contentpacket, d_len);
				break;

			case PPG:
				print_ppg_data(contentpacket, d_len);
				break;

			case ACC:
				print_acc_data(contentpacket, d_len);
				break;

			case BREATH:
				print_breath_data(contentpacket, d_len);
				break;
			default:
				printf("Not define service id! \n");
				break;
			}
#endif
*/

			zmsg_destroy(&msg);
		}

	}


	if (zctx_interrupted) {
		printf("W: interrupt received, killing stream_agent thread...\n");
		zsocket_destroy(ctx, publisher);
		fclose(fd);
	}

}

/// Streaming session create
static void session_create(void *args, zctx_t *ctx, void *pipe)
{
	int verbose = 1;
	int heartbeat = *(int*)args;

	void *pull = zsocket_new(ctx, ZMQ_PULL);
	int rc = zsocket_bind(pull, "tcp://155.230.15.45:%d", STREAM_PORT_PULL);
	char *device_id2;
	if (rc != 0)
	{
		zclock_log("PULL bind() ok!");
	}
	while (true)
	{
		zmq_pollitem_t items[] = {
			{ pipe, 0, ZMQ_POLLIN, 0 },
			{ pull, 0, ZMQ_POLLIN, 0}
		};

		int rc = zmq_poll(items, 2, 100 * ZMQ_POLL_MSEC);

		if (rc == -1)	// block state..
			break;

		if (items[0].revents & ZMQ_POLLIN)
		{
			zmsg_t *recv_XMLmsg = zmsg_recv(pipe);

			if (!recv_XMLmsg)	// Interrupted
				break;

			if (verbose)
			{
				zclock_log("I: received XML Message");
			}


			char * XMLmessage = zmsg_popstr(recv_XMLmsg);
			char *subject = get_xml_information(XMLmessage, "postit>subject", ATTRIBUTE_TEXT, 0);
			//스트링 관련 XML만 처리..
			if (streq(subject, "STREAMING_REQUEST")) {


#if defined(DEBUG_MODE)
				printf("------------------------------------\n");
				zclock_log("I[Streaming agent][XML receive Thread] : Received XML Debug");
				printf("recv xml : %s \n", XMLmessage);
#endif
				char *device_id = get_xml_information(XMLmessage, "postit>content>DeviceId", ATTRIBUTE_TEXT, 0);
				device_id2 = device_id;
				//strcpy(device_id2, device_id);
#if defined(DEBUG_MODE)
				printf("xml device_id : %s \n", device_id);
#endif
				//ServiceRequest, ServiceStop
				char *command = get_xml_information(XMLmessage, "postit>content>Command", ATTRIBUTE_TEXT, 0);
#if defined(DEBUG_MODE)
				printf("xml command : %s \n", command);
#endif
				//select 미구현
				char *select = get_xml_information(XMLmessage, "postit>content>Select", ATTRIBUTE_TEXT, 0);
#if defined(DEBUG_MODE)
				printf("xml select : %s \n", select);
#endif		
				char *sub_id = get_xml_information(XMLmessage, "postit>content>Identity", ATTRIBUTE_TEXT, 0);
#if defined(DEBUG_MODE)
				printf("sub identity : %s \n", sub_id);
				printf("------------------------------------\n");
#endif	

				if (streq(command, "ServiceRequest"))
				{
					// request a service

					// table insert fuction...
					int result = insert_key_item(z_hash, z_list, sub_id, &count, &config_flag);
					if (result == 0)
						printf("insert sucess...\n");
					else
						printf("insert fail...\n");

					send_stream_request(device_id);


				}
				else if (streq(command, "ServiceStop"))
				{
					if (count < 2)
						send_stream_stop(device_id);
				}
				else if (streq(command, "SavedDataListRequest"))
				{

				}
				else if (streq(command, "SavedDataRequest"))
				{

				}
				else
				{
					printf("not define cmd..\n");
				}
			}
		}

		if (items[1].revents & ZMQ_POLLIN)
		{
			char *iden = zstr_recv(pull);
			//	printf("i[pull] recv : %s\n", iden);
			int result = update_key_item(z_hash, z_list, iden, &count, &config_flag);
			if (result == 0)
				printf("no fined key...\n");
			else
				printf("fined key and update ...\n");

		}

		//현재 시간과 비교하여 table에 10sec 이상 차이 나는 key 있으면 삭제 & count 감소.
		if (rc == 0)
		{
			check_table(z_hash, z_list, &count, &config_flag);
			//하나의 요청 혹은 update가 들어오면 config_flag는 1이 된다.
			//config_flag = 1이 되면 count를 보고 count가 0이면 stop을 날린다
			if (config_flag == 1)
			{
				if (count < 1)
				{
					send_stream_stop(device_id2);
					config_flag = 0;
					printf("device_id2 : %s\n", device_id2);
				}
			}
		}
	}
	if (zctx_interrupted) {
		zclock_log("W: interrupt received, killing session create thread...");
		zsocket_destroy(ctx, pull);
	}
}
/*
static void main_handler(void *args, zctx_t *ctx, void *pipe)
{

	int verbose = 1;
	//struct th_arg *pa = (struct th_arg *)args;
	struct th_args temp;
	temp.heartbeat = 0;
	temp.se = NULL;
	temp = *(struct th_args*)args;
	int heartbeat = temp.heartbeat;
	mdp_worker_t *session = temp.se;



}
*/
int main(int argc, char *argv[])
{
	const char *broker, *name;
	int verbose = 0;
	//struct th_args arg;

	//timer는 쓰지 않고 polling으로 한다..
	/*
	struct itimerval tv;
	tv.it_interval.tv_sec = 1;
	tv.it_interval.tv_usec = 0;
	tv.it_value.tv_sec = 1;
	tv.it_value.tv_usec = 0;

	setitimer(ITIMER_REAL, &tv, NULL);

	//signal(SIGALRM, timeout);
	//register alarm signal..
	struct sigaction act;

	act.sa_handler = timeout;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	if (sigaction(SIGALRM, &act, 0) == -1)
		printf("sigaction error..\n");
		*/
		//    verbose = (argc > 3 && streq (argv [3], "-v"));	
	switch (argc) {
	case 1:
		broker = LOCAL_BROKER_ADDR;
		name = "stream_agent";
		break;
	case 3:
	case 4:
		broker = argv[1];
		name = argv[2];
		break;
	default:
		fprintf(stderr, "Usage: %s broker name [-v]\n", argv[0]);
		exit(1);
	}

	mdp_worker_t *session = mdp_worker_new(
		(char*)broker, (char*)STREAMING_AGENT, (char*)name, verbose);

#if defined(DEBUG_MODE)
	if (session != NULL)
		printf("worker create ok..\n");
#endif

	zctx_t* ctx = mdp_worker_get_context(session);
	int heartbeat = mdp_worker_get_heartbeat(session);

	//arg.heartbeat = heartbeat;
	//arg.se = session;

	//void *event = zthread_fork(ctx, main_handler, (void*)&arg);
	//error! zmq_proxy is return only ctx destory
	//	void *proxy_th = zthread_fork(ctx, proxy_thread, (void*)&heartbeat);
	//zmq_proxy(xsub, xpub, ps_monitor);

	// 내부 쓰레드를 만든다.

	void* pipe = zthread_fork(ctx, streaming_agent, (void*)&heartbeat);
	void* agent = zthread_fork(ctx, session_create, (void*)&heartbeat);


	while (1) {

		zframe_t *reply_to;
		zmsg_t *reply;
		zmsg_t *request = mdp_worker_recv(session, &reply_to);
		if (request == NULL)
			break;

		//zmsg_dump(request);
		// 자식 쓰레드로 메시지 전송
		zframe_t* command = zmsg_pop(request);

		if (zframe_streq(command, "XML_Message"))
		{
			//printf("recv xml : %s \n", request); 
			zmsg_send(&request, agent);
			reply = zmsg_new();
			zmsg_pushstr(reply, RET_CODE_OK);
			//for DEBUG

		}
		else if (zframe_streq(command, "recv"))
		{
			zmsg_send(&request, pipe);
			reply = zmsg_new();
			zmsg_pushstr(reply, RET_CODE_OK);
		}
		else
		{
			reply = zmsg_new();
			zmsg_pushstr(reply, RET_CODE_NOT_IMPLEMENTED);
		}

		// 클라이언트에 요청 서비스 결과 전송
		mdp_worker_send(session, &reply, reply_to);

		zframe_destroy(&reply_to);
		zmsg_destroy(&request);
	}

	mdp_worker_destroy(&session);


	zhash_destroy(&z_hash);
	zlist_destroy(&z_list);

	return 0;
}
