package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    String TAG = SimpleDhtProvider.class.getSimpleName();
    private final Uri myUri  = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String[] PORTS = {REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
    static final int SERVER_PORT = 10000;
    ArrayList<Node> list = new ArrayList<Node>();
    static  String curPort ="";
    String predPort="";
    String succPort="";
    BlockingQueue<String> bq = new ArrayBlockingQueue<String>(1);
    BlockingQueue<String> bq2 = new ArrayBlockingQueue<String>(1);
    String sender="";
    List<String> listOfFiles= new ArrayList<String>();
    String result = null;


    //Reference - OnPTestClickListener
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        if(selection.contains("@") || selection.contains("*")){
          Log.d(TAG,"Delete @ query");

           File[] files  = getContext().getFilesDir().listFiles();

            for(File file:files){
                file.delete();

            }
        }
        else{

            Log.d(TAG,"Delete key: "+ selection);
            File dir =  getContext().getFilesDir();

            File f= new File(dir,selection);
            if(f.exists())
               f.delete();


        }



        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String file = values.getAsString("key");
        String record = values.getAsString("value");

        Context context = getContext();
        try {

            //file = genHash(file);
            //Reference
            //http://stackoverflow.com/questions/14376807/how-to-read-write-string-from-a-file-in-android
            Log.i(TAG,"Insert input key :"+file+ " record :" +record +" curr:"+curPort );
            String hk = genHash(file);
            if( (succPort.equals(""))|| (hk.compareTo(genHash(predPort)) >0 && hk.compareTo(genHash(curPort)) < 0) ||
                    (genHash(predPort).compareTo(genHash(curPort))>0 && (hk.compareTo(genHash(predPort))>0 || hk.compareTo(genHash(curPort))<0))  ){

                listOfFiles.add(file);

                OutputStreamWriter outputStreamWriter = new OutputStreamWriter(context.openFileOutput(file, Context.MODE_PRIVATE));
                outputStreamWriter.write(record);
                outputStreamWriter.close();

                Log.i(TAG,"After Insert @"+curPort+" key:"+file+ " record:" +record );

            }else{
                Log.i(TAG,"Insert: calling Succ:"+succPort+" key:"+file+ " record:" +record );
                ClientTask client_task = new ClientTask();
                String token = "insert#"+file+"#"+record;
                Log.i(TAG,"Insert: Token:"+token +" port:"+String.valueOf(Integer.parseInt(succPort)*2));

                client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, token, String.valueOf(Integer.parseInt(succPort)*2));
            }

            /*OutputStreamWriter outputStreamWriter = new OutputStreamWriter(context.openFileOutput(file, Context.MODE_PRIVATE));
            outputStreamWriter.write(record);
            outputStreamWriter.close();*/



        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        //Log.i("insert", values.toString());

        return null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        String readline =  null;
        Context context = getContext();
        MatrixCursor matCur = new MatrixCursor(new String[] {"key", "value"});

        try {
            Log.v(TAG,"query inside +=>"+curPort+" selection:"+selection);
            String[] list = getContext().fileList();

           /* if(selection.contains("*")){
                Log.e(TAG, "Selection:"+ selection);

                if(selectionArgs != null){
                    result = selectionArgs[1];
                }
                else {
                    result = "";
                }
                FileInputStream input = null;
                String read_line = null;
                String value = null;

               for (String file : getContext().fileList()) {
                    input = getContext().openFileInput(file);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));

                    while ((read_line = bufferedReader.readLine()) != null) {
                        value = read_line;
                    }
                    result = result + file + "%" + value.toString() + "%";
                }

                if(selectionArgs==null || !selectionArgs[0].equals("star")) {
                    String msg = "star#" + selection + "#" + succPort + "#" + curPort;
                    Log.v("star", "client msg:" + msg);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, String.valueOf(Integer.parseInt(curPort) * 2));
                    String str = bq2.take();
                    String record[] = str.split("#");
                    matCur.addRow(record);
                    return matCur;
                }else if(selectionArgs[0].equals("star")){
                    String msg = "star#" + selection + "#" + succPort + "#" + selectionArgs[1];
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, String.valueOf(Integer.parseInt(curPort) * 2));
                }


                //MatrixCursor mat = query(myUri,null,"@",null,null);

                //String msg="keyfound#"+selection+":"+readline1+"#"+succPort+"#"+selectionArgs[1];


                //intial code
                /*for (String file : list) {
                    InputStream inputStream = context.openFileInput(file);
                    if (inputStream != null) {

                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                        BufferedReader reader = new BufferedReader(inputStreamReader);
                        readline = reader.readLine();
                        Log.i(TAG,"for *:: File:"+file+"\t Value:"+readline);
                        String record[] = {file, readline};
                        matCur.addRow(record);
                        // return matCur;

                    } else {
                        Log.v("query", "inputStream is NULL");
                    }
                }
                //intial code


                return matCur;
            }*/
            if(selection.contains("@") || selection.contains("*")){
                for (String file : listOfFiles) {
                    InputStream inputStream = context.openFileInput(file);
                    if (inputStream != null) {

                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                        BufferedReader reader = new BufferedReader(inputStreamReader);
                        readline = reader.readLine();

                        Log.i(TAG,"for @:: File:"+file+"\t Value:"+readline);
                        String record[] = {file, readline};
                        matCur.addRow(record);
                        Log.d("Count", matCur.getCount() + "");
                        //return matCur;

                    } else {
                        Log.v("query", "inputStream is NULL");
                    }
                }

                if(selection.contains("*") && !(succPort.equals("") || succPort==null)){

                    String msg = "star#";
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,String.valueOf(Integer.parseInt(curPort)*2));

                    String keyvals = bq.take();

                    Log.d(TAG,"Query: star KeyVals Received: "+ keyvals);
                    String[] pairs = keyvals.split("-");
                    for(String p: pairs){

                        if(!p.equals("") && !(p==null)){
                            String[] kv = p.split(":");
                            String key = kv[0];
                            String val = kv[1];
                            String record[] = {key, val};

                            matCur.addRow(record);

                        }
                    }



                }

                return matCur;

            }
            else {
                //Normal Query
                String hash = genHash(selection);
                boolean cond = (succPort.equals("") )|| (hash.compareTo(genHash(predPort)) >0 && hash.compareTo(genHash(curPort)) < 0) ||
                        (genHash(predPort).compareTo(genHash(curPort))>0 && (hash.compareTo(genHash(predPort))>0 || hash.compareTo(genHash(curPort))<0));

                Log.v("query", "normal-selection:"+selection+" hash:"+hash+" currNode:"+genHash(curPort));
                Log.v("query", "curr:"+curPort+" pred:"+predPort+" succ:"+succPort+" pred:"+genHash(predPort));
                Log.v("query", "pred:"+hash.compareTo(genHash(predPort))+" curr:"+hash.compareTo(genHash(curPort)) +"selectionArgs:"+selectionArgs);

                //case1: present in the 1st query node

                if ( cond && (selectionArgs==null || !selectionArgs[0].equals("pass")))
                {   Log.v("query", "case1 inside:");
                    InputStream inputStream = context.openFileInput(selection);
                    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                    BufferedReader reader = new BufferedReader(inputStreamReader);
                    readline = reader.readLine();

                    String record[] = {selection, readline};
                    matCur.addRow(record);
                    Log.v("query", "case1 returned selection:"+selection+" readline:"+readline);
                    return matCur;

                }//case2: Not present in 1st query node calling succ
                else if(selectionArgs==null || !selectionArgs[0].equals("pass")){
                    Log.v("query", "case2 inside");
                    sender=curPort;
                    String msg="query#"+selection+"#"+succPort+"#"+sender;
                    Log.v("query", "case2 client msg:"+msg);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,String.valueOf(Integer.parseInt(curPort)*2));
                    String str = bq.take();
                    String record[] = str.split("#");
                    matCur.addRow(record);
                    return matCur;
                }
                //case3: intermediate not found
                else if(((selectionArgs!=null && selectionArgs[0].equals("pass"))) && !cond ){
                    Log.v("query", "case3 inside");
                    String msg="query#"+selection+"#"+succPort+"#"+selectionArgs[1];
                    Log.v("query", "case3 client msg:"+msg);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,String.valueOf(Integer.parseInt(curPort)*2));
                }
                //case4: found @ intermediate node
                else if(cond  && selectionArgs!=null && selectionArgs[0].equals("pass")){
                    InputStream inputStream = getContext().openFileInput(selection);
                    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                    BufferedReader reader = new BufferedReader(inputStreamReader);
                    String readline1 = reader.readLine();

                    String msg="keyfound#"+selection+":"+readline1+"#"+succPort+"#"+selectionArgs[1];

                    Log.v("query", "case4 client msg:"+msg);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,String.valueOf(Integer.parseInt(curPort)*2));
                }
                else{
                    Log.v("query", "chck3 inputStream & succ is NULL");
                }
            }

            //return matCur;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        //Log.v("query", selection);
        return matCur;

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        try {

            String myPort = getPortNum();
            Log.v(TAG,"my port is:"+myPort);
            //curPort = String.valueOf(Integer.parseInt(myPort)/2);
            curPort = getPort();
            if(curPort.equals("5554")){
                Node node = new Node(getPort(),genHash(curPort),null,null);
                list.add(node);

                Log.i(TAG,"5554 added to Node List: "+ list.size());
            }

            if(!curPort.equals("5554")){
                Thread.sleep(2000);
            }

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.v(TAG, "*** ServerSocket Created ***");

            if(!myPort.equals("11108")){
                //Log.i(TAG, "Creating ClientTask port:"+curPort);
                ClientTask client_task = new ClientTask();
                if(curPort.equals("5556")){
                    Log.i(TAG, "Calling ClientTask port:"+curPort);
                    client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Node_Join#5556",  REMOTE_PORT0);
                }
                else if(curPort.equals("5558")){
                    Log.i(TAG, "Calling ClientTask port:"+curPort);
                    client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Node_Join#5558",  REMOTE_PORT0);
                }
                else if(curPort.equals("5560")){
                    Log.i(TAG, "Calling ClientTask port:"+curPort);
                    client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Node_Join#5560",  REMOTE_PORT0);
                }
                else if(curPort.equals("5562")){
                    Log.i(TAG, "Calling ClientTask port:"+curPort);
                    client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Node_Join#5562",  REMOTE_PORT0);
                }

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    //Reference PA2
    //eg:11108
    private String getPortNum() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String portNum = String.valueOf((Integer.parseInt(portStr) * 2));;
        return portNum;
    }
    //eg:5554
    private String getPort() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String portNum = String.valueOf((Integer.parseInt(portStr) ));;
        return portNum;
    }

    //ServerTask
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];


            while(true){
                try {

                    Socket socket = serverSocket.accept();
                    socket.setSoTimeout(50);
                    Log.d(TAG,"Testing at Server @"+curPort);
                    //String failedProc=null;

                    DataOutputStream dataOut;
                    BufferedReader dataIn = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    String msg2 = dataIn.readLine();
                    Log.d(TAG,"Entered server task:" +msg2);
                    if(msg2.startsWith("Node")){

                        Log.d(TAG,"Entered Server Task: Node join:"+msg2);
                        Log.i(TAG, curPort+"before");
                        Log.i(TAG, curPort+" Server Side Msg Received: " + msg2);

                        //Log.i(TAG,"Server msg : "+msg);

                        String[] input = msg2.split("#");
                        Log.i(TAG, "Server Side::input " + input[0]+"::"+input[1]);
                        if (input[0].equals("Node_Join")) {

                            //dataOut = new DataOutputStream(socket.getOutputStream());
                            //dataOut.writeUTF("ack");

                            Log.i(TAG, "Server Side::Inside " + input[0]+ " :"+list.size());
                            //String port = getPort();
                            if(list.size()==1) {
                                Log.i(TAG, "(1)Server Side::list.size(): " + list.size());
                                Node p = list.get(0);
                                //list.remove(p);
                                Node n = new Node(input[1], genHash(input[1]), p, p);

                                p.setPred(n);
                                p.setSucc(n);

                                list.add(n);
                                //list.add(p);
                                Log.i(TAG, "Server Side::list.size() after: " + list.size());

                            }
                            else if(list.size()>1)
                            {
                                Log.i(TAG, "(2)Server Side::list.size():" + list.size());
                                Node curNode = list.get(0);
                                Log.i(TAG, "curNode.getNode_id():" +curNode.getNode_id());
                                String newNode = genHash(input[1]);

                                while(true){
                                    //Log.i(TAG, "newNode.getNode_id():" +newNode);
                                    //Log.i(TAG, "curNode.getNode_id():" +curNode.getNode_id());
                                    //Log.i(TAG, "curNode.getPred().getNode_id():" +curNode.getPred().getNode_id());
                                    //Log.i(TAG, "curNode.getSucc().getNode_id():" +curNode.getSucc().getNode_id());
                                    if(curNode.getNode_id().compareTo(curNode.getPred().getNode_id())<0){
                                        if(newNode.compareTo(curNode.getPred().getNode_id())>0 ||
                                                newNode.compareTo(curNode.getNode_id())<0){

                                            Node n = new Node(input[1],newNode,curNode.getPred(),curNode);

                                            curNode.getPred().setSucc(n);
                                            curNode.setPred(n);

                                            list.add(n);


                                            Log.i(TAG, "(2)Server Side::special " + list.size());
                                            break;
                                        }
                                    }
                                    if(newNode.compareTo(curNode.getPred().getNode_id())>0 && newNode.compareTo(curNode.getNode_id())<0)
                                    {
                                        Node n = new Node(input[1],newNode,curNode.getPred(),curNode);

                                        curNode.getPred().setSucc(n);
                                        curNode.setPred(n);

                                        list.add(n);

                                        Log.i(TAG, "(2)Server Side::normal " + list.size());
                                        break;
                                    }
                                    curNode=curNode.getSucc();
                                }
                                for(int j=0; j<list.size();j++){
                                    Log.i(TAG, "Iter:" + j);
                                    Log.i(TAG, "portNum:" + list.get(j).portNum);
                                    //Log.i(TAG, "Node_id:" + list.get(j).Node_id);
                                    Log.i(TAG, "getPred:" + list.get(j).getPred().getPortNum());
                                    Log.i(TAG, "getSucc:" + list.get(j).getSucc().getPortNum());
                                }

                                Log.i(TAG, "Server Side::list.size() after:" + list.size());
                            }
                        }

                        dataOut = new DataOutputStream(socket.getOutputStream());
                        String str="start:";
                        for(int i=0; i<list.size(); i++){
                            str=str+list.get(i).getPortNum()+"-"+list.get(i).getPred().getPortNum()+"-"+list.get(i).getSucc().getPortNum()+"#";
                        }
                        Log.i(TAG,"String after second node: " + str);
                        dataOut.writeBytes(str+"\n");
                        dataOut.flush();
                        socket.close();

                    }
                    //
                    else if(msg2.startsWith("update")) {
                        dataIn = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//                        String msg = (String) dataIn.readLine();
                        String msg = msg2;
//                        update#predPort#succ#senderPort
//                        update#predPort#succ#senderPort
                        String[] out = msg.split("#");
//                        if (curPort.equals(out[0])) {
                        if (out[2].equals("prec")) {
                            Log.i(TAG, "predPort before update:" + predPort);
                            predPort = out[3];
                            Log.i(TAG, "predPort after updated:" + predPort);
                        }
                        if (out[2].equals("succ")) {
                            Log.i(TAG, "succPort before update:" + succPort);
                            succPort = out[3];
                            Log.i(TAG, "succPort after updated:" + succPort);
                        }
                        dataOut = new DataOutputStream(socket.getOutputStream());
                        dataOut.writeBytes("ack\n");
                        dataOut.flush();
                        dataOut.close();
                        dataIn.close();
                        socket.close();
//                        }
                        //
                        //socket.close();
                    } else if(msg2.startsWith("insert")){
                        //"insert#"+file+"#"+record;
                        String[] in = msg2.split("#");
                        ContentValues values= new ContentValues();

                        Context context = getContext();
                        values.put("key",in[1]);
                        values.put("value",in[2]);

                        dataOut = new DataOutputStream(socket.getOutputStream());
                        dataOut.writeBytes("insertack\n");
                        dataOut.flush();
                        dataOut.close();
                        dataIn.close();
                        socket.close();
                        //socket.close();
                        //getContext().insert(myUri,values);
                        insert(myUri,values);
                    }else if(msg2.startsWith("query")){
                        //"query#"+selection+"#"+succPort+"#"+origPort;
                        Log.i(TAG, "Server:for query msg:" + msg2);
                        String[] in = msg2.split("#");
                        String[] args = new String[]{"pass",in[3]};
                        ContentValues values= new ContentValues();
                        Log.i(TAG, "Server:for query msg:" + args[0]+" "+args[1]);
                        dataOut = new DataOutputStream(socket.getOutputStream());
                        dataOut.writeBytes("queryack\n");
                        dataOut.flush();
                        dataOut.close();
                        dataIn.close();
                        socket.close();

                        query(myUri, null,in[1], args, null, null);
                        //BlockingQueue<String> bq = new ArrayBlockingQueue<String>(1);
                        //bq.put(in[1]+"");


                        //socket.close();
                        //getContext().insert(myUri,values);

                    }
                    else if(msg2.startsWith("keyfound")){
                        //"query#"+selection:value+"#"+succPort+"#"+curPort;
                        Log.i(TAG, "Server:for query msg:" + msg2+" Key found");

                        dataOut = new DataOutputStream(socket.getOutputStream());
                        dataOut.writeBytes("keyfoundack\n");
                        dataOut.flush();
                        dataOut.close();
                        dataIn.close();
                        socket.close();

                        String[] in = msg2.split("#");
                        String key = in[1].split(":")[0];
                        String value = in[1].split(":")[1];

                        bq.put(key+"#"+value);
                        //BlockingQueue<String> bq = new ArrayBlockingQueue<String>(1);
                        //bq.put(in[1]+"");

                    }
                    else if(msg2.startsWith("star")){
                        //"star#" + selection + "#" + succPort + "#" + orgPort;
                        /*Log.i(TAG, "Server:for star msg:" + msg2);
                        String[] args = new String[]{"pass"};
                        String[] in = msg2.split("#");*/

                        Log.d(TAG,"Star Query Received at: "+ curPort);
                        Cursor cursor = query(myUri, null, "@" , null, null, null);
                        String ack = "starack#";
                        if(cursor.moveToFirst()){
                            do{

                                String key = cursor.getString(cursor.getColumnIndex("key"));
                                String value = cursor.getString(cursor.getColumnIndex("value"));
                                ack += key +":" + value+"-";

                            }while (cursor.moveToNext());
                        }

                        ack += "#"+succPort;

                        dataOut = new DataOutputStream(socket.getOutputStream());
                        dataOut.writeBytes(ack+"\n");
                        dataOut.flush();
                        dataOut.close();
                        dataIn.close();
                        socket.close();




                        //BlockingQueue<String> bq = new ArrayBlockingQueue<String>(1);
                        //bq.put(in[1]+"");

                    }




                    Log.i(TAG,"Final values: " + curPort + " " + predPort + " " + succPort);

                }catch (EOFException e) {
                    // ... this is fine
                    Log.i(TAG,e+"This is weird");
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (NullPointerException e) {
                    e.printStackTrace();
                }

            }

        }

        protected void onProgressUpdate(String...strings) {

        }

    }
    //ServerTask ends

    //ClientTask
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
                String msgToSend = msgs[0];

                boolean check = true;

                //BufferedReader dataIn = null;

                //Send a message over the socket
                Log.i(TAG, "Inside Client msgToSend: " + msgToSend);

                if(msgToSend.startsWith("Node")) {
                    try {
                        DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                        dataOut.writeBytes(msgToSend + "\n");
                        dataOut.flush();
    //                dataOut.close();
                        Log.i(TAG, "Inside Client for Node:" + msgToSend);

                        BufferedReader dataIn = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String ack = dataIn.readLine();

                        if (ack == null)
                            throw new NullPointerException();
                        if (ack.startsWith("start")) {
                            socket.close();
                        }
    //                    }
    //                    catch(Exception e) {
    //                        Log.d(TAG,"Exception");
    //                    }
                        //"insert#"+file+"#"+record;
                        //


                        Log.i(TAG, "Entering: " + ack);
                        String[] output = ack.split(":")[1].split("#");
                        for (int i = 0; i < output.length; i++) {
                            String[] sub = output[i].split("-");
                            Log.d(TAG, "Checking curr Port: " + output[i] + " " + curPort);
                            Log.d(TAG, "Ports: " + sub[0] + " " + curPort);
                            if (sub[0].equals(curPort)) {
                                //Sending to prec

                                predPort = sub[1];
                                succPort = sub[2];
                                Log.i(TAG, "Current Port:" + curPort + "Pred Port:" + predPort + "Succ:" + succPort);

                                String remotePrecPort = String.valueOf(Integer.parseInt(sub[1]) * 2);
                                Log.i(TAG, "pred port is: " + remotePrecPort);
                                Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePrecPort));
                                DataOutputStream dataOut1 = new DataOutputStream(socket1.getOutputStream());

                                dataOut1.writeBytes("update#" + predPort + "#succ#" + curPort + "\n");
                                dataOut1.flush();

                                BufferedReader dataIn1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
                                String ack1 = dataIn1.readLine();
                                Log.i(TAG, "After update: " + ack1);
                                if (ack1.equals("ack")) {
                                    Log.i(TAG, "Received ack succ update");
                                    dataIn1.close();
                                    socket1.close();
                                }

                                //Sending to succ
                                //predPort = sub[1];
                                //succPort = sub[2];
                                String remoteSuccPort = String.valueOf(Integer.parseInt(sub[2]) * 2);

                                Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remoteSuccPort));
                                DataOutputStream dataOut2 = new DataOutputStream(socket2.getOutputStream());

                                dataOut2.writeBytes("update#" + succPort + "#prec#" + curPort + "\n");
                                dataOut2.flush();

                                BufferedReader dataIn2 = new BufferedReader(new InputStreamReader(socket2.getInputStream()));
                                String ack2 = dataIn2.readLine();
                                Log.i(TAG, "ack: " + ack2);
                                if (ack2.equals("ack")) {
                                    Log.i(TAG, "Socket 2 closed");
                                    socket2.close();
                                }

                                check = false;

                                Log.i(TAG, "Inside Client " + curPort + " ack:" + output[i]);
                            }

                        }
                        Log.i(TAG, "Final values: " + curPort + " " + predPort + " " + succPort);
                    }
                    catch(Exception e) {
                        Log.d(TAG,"Exception " + e.toString());
                    }
                }
                else if (msgToSend.startsWith("insert")) {
                    //String[] in = ack.split("#");
                    //int remotePort = Integer.parseInt(in[1]) * 2;
                    //Socket insertSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), remotePort);

                    DataOutputStream dataOut1 = new DataOutputStream(socket.getOutputStream());
                    Log.i(TAG,"Client:sending to server:"+msgToSend);
                    dataOut1.writeBytes(msgToSend+"\n");
                    dataOut1.flush();

                    BufferedReader din = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String ack1 = din.readLine();
                    Log.i(TAG,"After update: " + ack1);
                    if (ack1 == null)
                        throw new NullPointerException();
                    if(ack1.equals("insertack")){
                        Log.i(TAG,"Client:Received ack for update:"+ack1);
                        din.close();
                        socket.close();
                    }

                }
                else if (msgToSend.startsWith("query")) {

                    //"query#"+selection+"#"+succPort+"#"+sender;
                    String[] msg = msgToSend.split("#");
                    Log.i(TAG, "msg at client for query: " + msgToSend);
                    int portToSend = Integer.parseInt(msg[2]) * 2;
                    Log.v("query:client",Integer.toString(portToSend));
                    Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msg[2]) * 2);
                    DataOutputStream dataOut2 = new DataOutputStream(socket3.getOutputStream());

                    dataOut2.writeBytes(msgToSend+"\n");
                    dataOut2.flush();

                    BufferedReader dataIn2 = new BufferedReader(new InputStreamReader(socket3.getInputStream()));
                    String ack2 = dataIn2.readLine();
                    Log.i(TAG, "queryack: " + ack2);

                    if (ack2 == null)
                        throw new NullPointerException();

                    if (ack2.equals("queryack")) {
                        Log.i(TAG, "Client:Received ack for query:"+ack2+ " from:"+msg[2]);
                        dataIn2.close();
                        socket3.close();
                    }

                }
                else if (msgToSend.startsWith("keyfound")) {

                    //"query#"+selection+"#"+succPort+"#"+sender;
                    String[] msg = msgToSend.split("#");
                    Log.i(TAG, "msg at client for keyfound: " + msgToSend + ": "+msg[3]);
                    //Socket socket4 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msg[3].trim()) * 2);
                    Socket socket4 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msg[3])*2);
                    DataOutputStream dataOut4 = new DataOutputStream(socket4.getOutputStream());

                    BufferedReader dataIn2 = new BufferedReader(new InputStreamReader(socket4.getInputStream()));
                    Log.i(TAG, "msg at client for key found DataOupt: " + msgToSend + ": "+msg[3]);
                    dataOut4.writeBytes(msgToSend+"\n");
                    dataOut4.flush();
                    Log.i(TAG, "msg at client for keyfound Write Bytes: " + msgToSend + ": "+msg[3]);
                    String ack2 = null;
                    try{
                        ack2 = dataIn2.readLine();
                        Log.d(TAG, "keyfound readline: " + ack2);
                    }catch (Exception e) {
                        Log.d(TAG, "Timed out waiting for keyfound ack");
                    }

                    Log.i(TAG, "keyfound ack: " + ack2);

                    if (ack2 == null)
                        throw new NullPointerException();

                    if (ack2.startsWith("keyfoundack")) {
                        Log.i(TAG, "Client:Received ack for keyfound:"+ack2);
                        dataIn2.close();
                        socket4.close();
                    }

                }else if (msgToSend.startsWith("star")) {
                    //"star#" + selection + "#" + succPort + "#" + orgPort;


                    //String[] msg = msgToSend.split("#");
                    Log.i(TAG, "msg at client for star: " + msgToSend);
                    //int portToSend = Integer.parseInt(msg[2]) * 2;

                    //Log.v("star:client:", Integer.toString(portToSend));

                    String sendToPort = succPort;
                    String originalSender = curPort;
                    String pairs = "";

                    while (!sendToPort.equals(originalSender)) {

                        Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sendToPort) * 2);
                        DataOutputStream dataOut2 = new DataOutputStream(socket3.getOutputStream());

                        dataOut2.writeBytes(msgToSend + "\n");
                        dataOut2.flush();

                        BufferedReader dataIn2 = new BufferedReader(new InputStreamReader(socket3.getInputStream()));
                        String ack2 = dataIn2.readLine();
                        Log.i(TAG, "Client:starack: " + ack2);

                        if (ack2 == null)
                            throw new NullPointerException();

                        if (ack2.startsWith("starack")) {
                            //Log.i(TAG, "Client:Received ack for star:" + ack2 + " from:" + msg[2]);
                            String[] data = ack2.split("#");

                            Log.d(TAG,"Client: KeyVal Received from "+sendToPort+": Sending To :"+data[2].trim() +" "+ data[1] );

                            sendToPort = data[2].trim();


                            pairs += data[1];

                            dataIn2.close();
                            socket3.close();
                        }
                    }

                    bq.put(pairs);
                }

            } catch (Exception e) {
//                e.printStackTrace();

                Log.d(TAG,"KeyFoundException: "+e);
            }

            return null;
        }
    }
    //ClientTask ends


    public class Node {
        public String portNum;
        public String Node_id;
        public Node pred;
        public Node succ;

        public Node(String portNum, String Node_id, Node pred, Node succ){
            this.portNum = portNum;
            this.Node_id = Node_id;
            this.pred = pred;
            this.succ = succ;
        }

        public void setPortNum(String portNum){
            this.portNum = portNum;
        }
        public void setNode_id(String Node_id){
            this.Node_id = Node_id;
        }
        public void setPred(Node pred){
            this.pred = pred;
        }
        public void setSucc(Node succ){
            this.succ = succ;
        }


        public String getPortNum(){
            return this.portNum;
        }
        public String getNode_id(){
            return this.Node_id;
        }
        public Node getPred(){
            return this.pred;
        }
        public Node getSucc(){
            return this.succ;
        }

    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}