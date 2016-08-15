/*
 *  node.js add-on wrapping C++ url_classification code
 *  @author Jacinto Callu jdc@thevideogenomeproject.com
 *  @version M26
 *  @last_edited Jacinto Callu jdc@thevideogenomeproject.com
 */


#include <node.h>
#include <v8.h>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <mysql/mysql.h>

using namespace v8;
using namespace node;
using namespace std;

namespace mysql_sync {
  MYSQL *connection;

  unsigned int mq = 0;

  void end (const FunctionCallbackInfo<Value>& args){
    mysql_close(connection);
  }

  void connect(const FunctionCallbackInfo<Value>& args){
    connection = mysql_init(NULL);
    // Isolate* isolate = args.GetIsolate();
    Local<Value> hostN(args[0]);
    Local<Value> userN(args[1]);
    Local<Value> passwordN(args[2]);
    Local<Value> databaseN(args[3]);
    Local<Value> portN(args[4]);
    String::Utf8Value hostM(hostN->ToString());
    const char* host = *hostM;
    String::Utf8Value userM(userN->ToString());
    const char* user = *userM;
    String::Utf8Value passwordM(passwordN->ToString());
    const char* password = *passwordM;
    String::Utf8Value databaseM(databaseN->ToString());
    const char* database = *databaseM;
    int port = portN->NumberValue();

    connection = mysql_real_connect(connection,host,user,password,database,port,NULL,0);
    if( ! connection ){
      throw "Connection Failed";
    } else {
      mq = 1;
    }
  }

  void query(const FunctionCallbackInfo<Value>& args) {
      Isolate* isolate = args.GetIsolate();
      Local<Value> arg_one(args[0]); // input 1
      Local<Value> arg_two(args[1]); // callback
      v8::String::Utf8Value param1(arg_one->ToString());
      const char* param1_c_str = *param1;
      // cout << "JS input to c++ output in parens: ("+ param1_c_str+") " << endl;
      Local<Function> cb = Local<Function>::Cast(arg_two); //
      Local<Value> err = v8::Null(isolate);
      // Local<Array> result_list = Array::New(isolate);
      MYSQL_ROW row;
      MYSQL_RES *res_set;
      // std::vector<row> rows;
      try {
        unsigned int i = 0;
        mysql_query(connection,param1_c_str);
        res_set = mysql_store_result(connection);
        unsigned int numrows = mysql_num_rows(res_set);
        cout << numrows << " Tables in " << 'test' << " database " << endl;
        while (((row=mysql_fetch_row(res_set)) !=NULL)){
          // rows.push_back(
          //   unpack_location(isolate,Local<Object>::Cast(row->Get(i)));
          // );
          cout << row[i] << endl;
        }
      }
      catch(const char* msg){
        err = Exception::Error( String::NewFromUtf8(isolate, msg) );
      }

      // Local<Array> output = res_set;

      const unsigned argc = 2;
      Local <Value> argv[argc] = {
         err,
         Null(isolate)
       };
      // cout << "Before callback call" << endl;
      cb->Call( isolate->GetCurrentContext()->Global(), argc, argv);
  }
  void isConnected(const FunctionCallbackInfo<Value>& args){
    // Isolate* isolate = args.GetIsolate();

    if( ! connection ){
      args.GetReturnValue().Set(false);
    } else {
      args.GetReturnValue().Set(true);
    }

  }
  void init(Local<Object> module) {
      NODE_SET_METHOD(module, "query", query);
      NODE_SET_METHOD(module, "connect", connect);
      NODE_SET_METHOD(module, "end", end);
      NODE_SET_METHOD(module, "isConnected", isConnected);
  }

  NODE_MODULE(mysql_sync, init)
}
