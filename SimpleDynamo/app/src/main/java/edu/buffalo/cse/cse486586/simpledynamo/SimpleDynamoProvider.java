package edu.buffalo.cse.cse486586.simpledynamo;

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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleDynamoProvider extends ContentProvider {
	final String[] ports = {"11124", "11112", "11108", "11116", "11120"};
	private String myPort;
	static final int SERVER_PORT = 10000;
	static final String TAG = SimpleDynamoActivity.class.getSimpleName();
	private ContentResolver xContentResolver;
	private static String myKey = "";
	private static String myPred = "";
	private static String mySuc="";
	private static String mySuc2 = "";
	private final Uri mUri2 = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	static final Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
	public volatile static boolean busyInserting = false;
	private static final String VALUE_FIELD = "value";
	private static final String KEY_FIELD = "key";
	private static Map<String,msg> mainStorage = new ConcurrentHashMap<String,msg>();
    private static ArrayList<String> hashedArray = new ArrayList<String>();
    private static ArrayList<String> origArray = new ArrayList<String>();
    private static HashMap<String,String> hashToPort = new HashMap<String, String>();
    private static Map<String,Integer> keyVersion = new ConcurrentHashMap<String,Integer>();
    private volatile static boolean busyQuery = false;
    private volatile static boolean blockAll = false;
    private volatile static boolean failed = false;
    private volatile static String failedPort = "";
    private volatile static String failedSuc1 = "";
    private volatile static String failedSuc2 = "";
    private volatile static String origSuc = "";
    private volatile static String origSuc2 = "";
    private volatile static String orgPred = "";
    private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

    public static String getSuc(String xboy){

        String mBoy = String.valueOf(Integer.parseInt(xboy)/2);
        String boy = "";
        try {
            boy = genHash(mBoy);
        } catch (Exception x) {
            Log.e("Failure Algo", "Exception");
        }

        for(int i=0;i<origArray.size();i++){
            if(origArray.get(i).equals(boy)){
                if(i==4){
                    return origArray.get(0);
                }
                else{
                    int x=i+1;
                    return origArray.get(x);
                }

            }
        }
        return null;
    }
    public static String findOwner(String hashedKey,ArrayList<String> har){
        boolean found=false;
        Collections.sort(har);
        if ((hashedKey.compareTo(har.get(0)) <= 0) || (hashedKey.compareTo(har.get(har.size() - 1)) >= 1)) {

                /*Find out port 1*/
            Collections.sort(har);
            String hash1 = har.get(0);
            String port1 = hashToPort.get(hash1);
            found = true;
            return port1;
        }


        if (found == false) {
            for (int i = 0; i < har.size(); i++) {
                if ((hashedKey.compareTo(har.get(i)) <= 0) && hashedKey.compareTo(har.get(i - 1)) >= 1) {
                    String thisPort = hashToPort.get(har.get(i));
                    return thisPort;
                }
            }
        }

        return null;
    }

    public class msg{
        String port;
        String key;
        String value;

                public msg(String _key,String _value,String _port){
                    this.key=_key;
                    this.value=_value;
                    this.port=_port;
                }
    }

    public static void checkPosition(ArrayList<String> pass){
        /*Sort the arrayList inorder to get positions*/
        Collections.sort(pass);

        String pred="";
        String suc="";
        String suc2="";

        for(int i=0;i<pass.size();i++){

            if(pass.get(i).equals(myKey)){
                if(i==0){
                    pred = pass.get(pass.size()-1);
                    suc = pass.get(1);
                    suc2 = pass.get(2);
                    break;
                }
                else if(i==pass.size()-1){
                    int x = i;
                    pred = pass.get(x-1);
                    suc = pass.get(0);
                    suc2 = pass.get(1);
                    break;
                }
                else if(i==pass.size()-2){
                    int x = i;
                    pred = pass.get(x-1);
                    suc = pass.get(x+1);
                    suc2 = pass.get(0);
                    break;
                }
                else{
                    int x = i;
                    pred = pass.get(x-1);
                    suc = pass.get(x+1);
                    suc2 = pass.get(x+2);
                    break;
                }
            }
        }
        myPred = pred;
        mySuc=suc;
        mySuc2 = suc2;
    }

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
            mainStorage =  new ConcurrentHashMap<String, msg>();
        try {
            new deleteAll().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, null, null);
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }
            Log.e("HashMap Size",String.valueOf(mainStorage.size()));

        if(selection.equals("@") || selection.equals("*")){
            mainStorage = new ConcurrentHashMap<String,msg>();
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
        while(true){
            if(blockAll==false)
                break;
        }

        busyInserting=true;
        String key = (String) values.get(KEY_FIELD);
        String value = (String) values.get(VALUE_FIELD);
        Log.e("insert revcd",key);
        Log.e("value recvd",value);
        String hashedKey = "";
        try {
            hashedKey = genHash(key);
        } catch (Exception e) {
            Log.e("Err Hashing", e.toString());
        }

        if ((myKey.compareTo(orgPred) < 0 && hashedKey.compareTo(orgPred) > 1) || (myKey.compareTo(orgPred) < 0 && hashedKey.compareTo(myKey) <= 0) || (hashedKey.compareTo(orgPred) > 1 && hashedKey.compareTo(myKey) <= 0)) {

            Log.e("inserted", key);
            msg temp = new msg(key,value,myPort);
            mainStorage.put(key,temp);
            if (!keyVersion.containsKey(key)) {
                keyVersion.put(key, 1);
            } else {
                int version = keyVersion.get(key);
                keyVersion.remove(key);
                keyVersion.put(key, version + 1);
            }


            String replicateMsg = "";
                replicateMsg = "REPLICATE" + " "+ myPort + " " + key + " " + value+" "+myPort;
                String port = hashToPort.get(origSuc);

            try {


                Log.e("Replicatiion-1 key", port);
                String msgToSend = replicateMsg;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
                BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                msgSend.write(msgToSend);
                msgSend.newLine();
                msgSend.flush();
                BufferedReader newReader = new BufferedReader((new InputStreamReader(socket.getInputStream())));
                String starResult = newReader.readLine();
                Log.e("Insert send", starResult);
                socket.close();

            } catch (Exception e) {
                Log.e("Error Replicating key", e.toString());
                Log.e("Sucessor before",mySuc);
                if(failed==false) {
                    blockAll = true;
                    failed = true;
                    failedPort = port;
                    blockAll = false;
                }

            }

            String portNew = hashToPort.get(origSuc2);

                try {

                    Log.e("Replica-2 key", portNew);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(portNew));
                    BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    msgSend.write(replicateMsg);
                    msgSend.newLine();
                    msgSend.flush();
                    BufferedReader newReader = new BufferedReader((new InputStreamReader(socket.getInputStream())));
                    String starResult = newReader.readLine();
                    Log.e("Insert send", starResult);
                    socket.close();

                } catch (Exception e) {
                    Log.e("Error Replcating-2 Key", e.toString());
                    Log.e("Error Replicating key", e.toString());
                    Log.e("Sucessor before",mySuc);
                    if(failed==false) {
                        blockAll = true;
                        failed = true;
                        failedPort = portNew;
                        blockAll = false;
                    }
                }

            busyInserting=false;
            return null;

        }
        else{

            /*Forward the key to the node where it belongs*/
            Collections.sort(hashedArray);
            String origPort = findOwner(hashedKey,origArray);
            String forwardPort = "INSERT" +" "+ origPort + " " + key + " " + value;

            boolean localFail = false;

                try {

                    Log.e("Forwarding key", origPort);
                    String msgToSend = forwardPort;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(origPort));
                    BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    msgSend.write(msgToSend);
                    msgSend.newLine();
                    msgSend.flush();
                    BufferedReader newReader = new BufferedReader((new InputStreamReader(socket.getInputStream())));
                    String starResult = newReader.readLine();
                    Log.e("Insert send", starResult);
                    socket.close();

                } catch (Exception e) {
                    Log.e("Error Forwarding Key FaILED", e.toString());
                    Log.e("Sucessor before",mySuc);

                    if(failed==false) {
                        blockAll = true;
                        failed = true;
                        failedPort = origPort;
                        blockAll = false;
                    }
                    localFail=true;

                }

            if(localFail==true){
                String portHash = getSuc(origPort);


                try {

                    String portNew = hashToPort.get(portHash);
                    Log.e("Forwarding key", portNew);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(portNew));
                    BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    msgSend.write(forwardPort);
                    msgSend.newLine();
                    msgSend.flush();
                    BufferedReader newReader = new BufferedReader((new InputStreamReader(socket.getInputStream())));
                    String starResult = newReader.readLine();
                    Log.e("Insert send", starResult);
                    socket.close();

                } catch (Exception e) {
                    Log.e("Error Forwarding Key", e.toString());

                    if(failed==false) {
                        blockAll = true;
                        failed = true;
                        failedPort = portHash;
                        blockAll = false;
                    }
                }
                localFail=false;


            }

            busyInserting=false;
        }
            busyInserting=false;
            return null;
	}

	@Override
	public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        xContentResolver = getContext().getContentResolver();
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        for(int i=0;i<ports.length;i++){
            String temp = ports[i];
            String insertKey = "";
            try {
                insertKey = genHash(String.valueOf(Integer.parseInt(temp)/2));
            } catch (Exception e) {
                Log.e("Err Hashing", e.toString());
            }
            hashedArray.add(insertKey);
            origArray.add(insertKey);
            hashToPort.put(insertKey,ports[i]);
        }
        try {
            myKey = genHash(String.valueOf(Integer.parseInt(myPort)/2));
        } catch (Exception e) {
            Log.e("Err Hashing", e.toString());
        }
        Log.e("My key", myKey);
        Collections.sort(hashedArray);
        checkPosition(hashedArray);
        orgPred= new String(myPred);
        origSuc = new String(mySuc);
        origSuc2 = new String(mySuc2);
        try {
            new myAlgoTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, null, null);
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        while(true){
            if(blockAll==false)
                break;
        }

        Log.e("Query revcd", selection);

        if(selection.equals("@")) {
            MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
            for (Map.Entry<String, msg> entry : mainStorage.entrySet()) {
                msg temp = entry.getValue();
                String value = temp.value;
                String key = temp.key;
                matrixCursor.addRow(new Object[]{key,value});
            }
            return matrixCursor;
        }
        else if(selection.equals("*")){
            MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
            ArrayList<String> starReplies = new ArrayList<String>();

            for (int i = 0; i < ports.length; i++) {
                String port = ports[i];

                String findKey = "STARQUERY" +" "+ myPort;
                try {
                    String portStar = ports[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(portStar));
                    BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    msgSend.write(findKey);
                    msgSend.newLine();
                    msgSend.flush();
                    BufferedReader newReader = new BufferedReader((new InputStreamReader(socket.getInputStream())));
                    String starResult = newReader.readLine();
                    if(starResult.contains("###")){
                        Log.e("Star Reply recvd","*");
                        String[] starMsg = starResult.split("###");

                        for(int k=0;k<starMsg.length;k++){
                            String[] tempMsg = starMsg[k].split("\\s+");
                            String returnKey = tempMsg[0];
                            String returnValue = tempMsg[1];
                            Log.e(returnKey,returnValue);
                            starReplies.add(returnKey);
                            starReplies.add(returnValue);
                        }
                    }
                    msgSend.flush();
                    socket.close();
                } catch (Exception e) {
                    Log.e("Error Star Failed", e.toString());
                }

            }

            for (int i = 0; i < starReplies.size() - 1; i+=2) {
                Log.e(starReplies.get(i)+" ",starReplies.get(i+1));
                matrixCursor.addRow(new Object[]{starReplies.get(i), starReplies.get(i + 1)});
            }
            return matrixCursor;
        }
        else {
            String hashedKey = "";
            try {
                hashedKey = genHash(selection);
            } catch (Exception e) {
                Log.e("Err Hashing", e.toString());
            }

            if ((myKey.compareTo(myPred) <= 0 && hashedKey.compareTo(myPred) >= 1) || (myKey.compareTo(myPred) <= 0 && hashedKey.compareTo(myKey) <= 0) || (hashedKey.compareTo(myPred) >= 1 && hashedKey.compareTo(myKey) <= 0) ) {

                MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
                msg temp = mainStorage.get(selection);
                String value = temp.value;
                matrixCursor.addRow(new Object[]{selection, value});
                /*Get quorum*/
                busyQuery=false;
                return  matrixCursor;
            }
            else{
                boolean notFound=false;

                String keyR = "";
                String valueR="";
                String portFind = findOwner(hashedKey,origArray);
                Log.e("Found owner",portFind);
                String findKey = "";

                    findKey = "FIND" + " " + selection + " " + myPort;
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(portFind));
                    BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    msgSend.write(findKey);
                    msgSend.newLine();
                    msgSend.flush();
                    BufferedReader newReader = new BufferedReader((new InputStreamReader(socket.getInputStream())));
                    String starResult = newReader.readLine();
                    socket.close();
                        Log.e("Query Reply recvd",starResult);
                        String[] replyMsg = starResult.split("\\s+");
                        String key = replyMsg[1];
                        valueR  = replyMsg[2];
                } catch (Exception e) {
                    Log.e("Find Exception Caught", e.toString());
                    notFound =true;
                }

                if (notFound==true){
                    String suTemp = getSuc(portFind);
                    String portFind2 = hashToPort.get(suTemp);

                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portFind2));
                        BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                        msgSend.write(findKey);
                        msgSend.newLine();
                        msgSend.flush();
                        BufferedReader newReader = new BufferedReader((new InputStreamReader(socket.getInputStream())));
                        String starResult = newReader.readLine();
                        socket.close();
                        Log.e("Query Reply recvd",starResult);
                        String[] replyMsg = starResult.split("\\s+");
                        String key = replyMsg[1];
                        valueR  = replyMsg[2];
                    } catch (Exception e) {
                        Log.e("Find Exception Caught", e.toString());
                        notFound =true;
                    }

                }



                MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
                matrixCursor.addRow(new Object[]{selection, valueR});
                return matrixCursor;
            }


            }
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            do {
                try {


                    Socket incomingConnection = serverSocket.accept();
                    BufferedReader newReader = new BufferedReader(new InputStreamReader(incomingConnection.getInputStream()));
                    String incomingMessage = newReader.readLine();
                    Log.e("Recvd this", incomingMessage);

                    BufferedWriter newWriter = new BufferedWriter(new OutputStreamWriter(incomingConnection.getOutputStream()));

                    if(incomingMessage.contains("INSERT")){
                        newWriter.write("INDONE");
                        newWriter.newLine();
                        newWriter.flush();
                        String[] insertMsg = incomingMessage.split("\\s+");
                        msg temp = new msg(insertMsg[2],insertMsg[3],insertMsg[1]);
                        mainStorage.put(insertMsg[2], temp);
                        String replicateMsg = "REPLICATE" + " "+ myPort + " " + insertMsg[2] + " " + insertMsg[3]+" "+"THISMINE";
                        if(insertMsg[1].equals(myPort)) {
                            publishProgress(replicateMsg);
                        }
                        else{
                            String replicateMsg2 = "ONCE" + " "+ insertMsg[1] + " " + insertMsg[2] + " " + insertMsg[3]+" "+"NOTMINE";
                            publishProgress(replicateMsg2);
                        }
                    }

                    if(incomingMessage.contains("JOIN")){
                        String[] joinArray = incomingMessage.split("\\s+");
                        String hashedPort = joinArray[1];
                        String recPort = joinArray[2];
                        String intPort = String.valueOf(Integer.parseInt(hashedPort) / 2);
                        try {
                            hashedPort = genHash(intPort);
                        } catch (Exception x) {
                            Log.e("Failure Algo", "Exception");
                        }

                        if(failed==true) {
                            blockAll = true;
                            failed = false;
                            blockAll = false;
                        }

                            Log.e("Recvd suc",joinArray[1]);
                            Log.e("My port", myPort);
                            Log.e("My pred", hashToPort.get(orgPred));

                            if(hashedPort.equals(orgPred)){

                                StringBuilder star = new StringBuilder();

                                for (Map.Entry<String, msg> entry : mainStorage.entrySet()) {
                                    msg temp  = entry.getValue();
                                    String key = entry.getKey();
                                    String value = temp.value;
                                    String port = temp.port;
                                    Log.e("Key",key);
                                    Log.e("Port of key",port);
                                    if(port.equals(recPort) || port.equals(joinArray[1])) {
                                        star.append(key);
                                        star.append(" ");
                                        star.append(value);
                                        star.append(" ");
                                        star.append(temp.port);
                                        Log.e("Key", key);
                                        Log.e("Value", value);
                                        star.append("###");
                                    }
                                }


                                String temp = star.toString();
                                Log.e("String formed",temp);
                                if(temp.length()>0) {
                                    Log.e("Sending formation",temp);
                                    String sendThis =  temp.substring(0,temp.length()-3);
                                    newWriter.write(sendThis);
                                    newWriter.newLine();
                                    newWriter.flush();
                                }
                                else{
                                    newWriter.write("GTIT");
                                    newWriter.newLine();
                                    newWriter.flush();
                                }

                            }

                        else{
                            newWriter.write("GTIT");
                            newWriter.newLine();
                            newWriter.flush();
                        }


                    }

                    if(incomingMessage.contains("DELETEALL")){
                        mainStorage = new ConcurrentHashMap<String, msg>();
                    }

                    if(incomingMessage.contains("PREDEC")){
                        newWriter.write(hashToPort.get(myPred));
                        newWriter.newLine();
                        newWriter.flush();
                    }

                    if(incomingMessage.contains("QUORUM")){
                        String[] quorumArray = incomingMessage.split("\\s+");
                        String key = quorumArray[1];
                        msg temp = mainStorage.get(key);
                        String value = temp.value;
                        int version = keyVersion.get(key);
                        String reqReply = "QREPLY"+" "+key+" "+value+" "+version;
                        newWriter.write(reqReply);
                        newWriter.newLine();
                        newWriter.flush();
                    }
                    if(incomingMessage.contains("FIND")){

                        String[] findMsg = incomingMessage.split("\\s+");
                        msg temp = mainStorage.get(findMsg[1]);
                        String returnValue = temp.value;
                       /* Get quorum here*/

                        String reqReply = "QUERYREPLY"+" "+findMsg[1]+" "+returnValue;
                        newWriter.write(reqReply);
                        newWriter.newLine();
                        newWriter.flush();
                    }

                    if(incomingMessage.contains("REPLICATE")) {
                        newWriter.write("GTIT");
                        newWriter.newLine();
                        newWriter.flush();
                        String[] replicateArray = incomingMessage.split("\\s+");
                        String key = replicateArray[2];
                        if (!keyVersion.containsKey(key)) {
                            keyVersion.put(key, 1);
                        } else {
                            int version = keyVersion.get(key);
                            keyVersion.remove(key);
                            keyVersion.put(key, version + 1);
                        }
                        msg temp = new msg(replicateArray[2],replicateArray[3],replicateArray[1]);
                        mainStorage.put(key,temp);
                    }

                    if(incomingMessage.contains("REQUEST")){

                            StringBuilder star = new StringBuilder();
                            for (Map.Entry<String, msg> entry : mainStorage.entrySet()) {
                                msg temp = entry.getValue();
                                String key = temp.key;
                                String value = temp.value;

                                    star.append(key);
                                    star.append(" ");
                                    star.append(value);
                                    star.append(" ");
                                    star.append(temp.port);
                                    star.append("###");
                            }

                            String temp = star.toString();
                        if(temp.length()>0) {
                            String sendThis = temp.substring(0, temp.length() - 3);
                            newWriter.write(sendThis);
                            newWriter.newLine();
                            newWriter.flush();
                        }
                        else{
                            newWriter.write("GTIT");
                            newWriter.newLine();
                            newWriter.flush();
                        }

                    }

                    if(incomingMessage.contains("STARQUERY")){
                        Log.e("Got star", "query");
                        /************ Query your database, form a string and reply back *********/
                        String[] starMsg = incomingMessage.split("\\s+");

                        StringBuilder star = new StringBuilder();
                        for (Map.Entry<String, msg> entry : mainStorage.entrySet()) {
                            String key = entry.getKey();
                            msg temp = entry.getValue();
                            String value = temp.value;
                            star.append(key);
                            star.append(" ");
                            star.append(value);
                            star.append("###");
                        }
                        String temp = star.toString();
                        String sendThis =  temp.substring(0,temp.length()-3);
                        newWriter.write(sendThis);
                        newWriter.newLine();
                        newWriter.flush();
                        Log.e("Replied to", "*");
                    }
                    newReader.close();
                    incomingConnection.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            } while (true);
        }

        private synchronized boolean testInsert(ContentValues cv) {
            try {
                xContentResolver.insert(mUri2, cv);
            } catch (Exception e) {
                Log.e(TAG, e.toString());
                return false;
            }

            return true;
        }

        protected void onProgressUpdate(String... strings) {
            String strReceived = strings[0];
            String[] splitStr = strReceived.split("\\s+");

            if(strReceived.contains("ONCE")){
                String[] starMsg = strReceived.split("\\s+");
                String msgS = "REPLICATE"+" "+starMsg[1]+" "+starMsg[2]+" "+starMsg[3]+" "+starMsg[4];
                try {
                    new sendToPort().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgS, hashToPort.get(origSuc));
                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
            else {
                try {
                    new forwarder().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, strReceived, hashToPort.get(origSuc));
                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }


            return;
        }
    }
    private class myAlgoTask extends AsyncTask<String, String, Void> {
        private synchronized boolean testInsert(ContentValues cv) {
            try {
                xContentResolver.insert(mUri2, cv);
            } catch (Exception e) {
                Log.e(TAG, e.toString());
                return false;
            }

            return true;
        }

        @Override
        protected Void doInBackground(String... msgs) {

            blockAll=true;
            Log.e("Algo task","Activated");

            /******** Send a joining msg from here **************************/
            for(int i=0;i<ports.length;i++) {

                if(ports[i].equals(myPort))
                    continue;

                try {
                    String port = ports[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    String msgToSend = "JOIN"+" "+myPort+" "+hashToPort.get(orgPred);
                    BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    msgSend.write(msgToSend);
                    msgSend.newLine();
                    msgSend.flush();
                    BufferedReader newReader = new BufferedReader((new InputStreamReader(socket.getInputStream())));
                    String temp = newReader.readLine();
                    Log.e("Recovered", temp);

                    if(temp.contains("###")){

                        String[] starMsg = temp.split("###");

                        for(int k=0;k<starMsg.length;k++){
                            String[] tempMsg = starMsg[k].split("\\s+");
                            String returnKey = tempMsg[0];
                            String returnValue = tempMsg[1];
                            Log.e(returnKey, returnValue);
                                msg tempIn = new msg(returnKey, returnValue, tempMsg[2]);
                                mainStorage.put(returnKey, tempIn);
                        }

                    }
                    socket.close();
                } catch (Exception e) {
                    Log.e("Error JOIN", e.toString());
                }
            }

            String req2="";
            try {
                String port = hashToPort.get(orgPred);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
                String msgToSend = "PREDEC";
                BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                msgSend.write(msgToSend);
                msgSend.newLine();
                msgSend.flush();
                BufferedReader newReader = new BufferedReader((new InputStreamReader(socket.getInputStream())));
                String temp = newReader.readLine();
                Log.e("Got Pred-Pred", temp);
                req2 = new String(temp);
                socket.close();
            } catch (Exception e) {
                Log.e("Exception Gettin Pred", e.toString());
            }

            try {
                String port = req2;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
                String msgToSend = "REQUEST";
                BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                msgSend.write(msgToSend);
                msgSend.newLine();
                msgSend.flush();
                BufferedReader newReader = new BufferedReader((new InputStreamReader(socket.getInputStream())));
                String temp = newReader.readLine();
                if(temp.contains("###")) {

                    ArrayList<String> starReplies = new ArrayList<String>();
                    String[] starMsg = temp.split("###");
                    for (int k = 0; k < starMsg.length; k++) {
                        String[] tempMsg = starMsg[k].split("\\s+");
                        String returnKey = tempMsg[0];
                        String returnValue = tempMsg[1];
                        String returnedPort = tempMsg[2];
                        Log.e(returnKey, returnValue);

                        if(returnedPort.equals(req2)) {
                            msg tempIn = new msg(returnKey, returnValue, req2);
                            mainStorage.put(returnKey, tempIn);
                        }
                    }

                }
                socket.close();
            } catch (Exception e) {
                Log.e("Exception fetching replica 2", e.toString());
            }

            /*******************Sending Join block ends here************************/

            blockAll=false;
            return null;
        }


    }
    private class sendToPort extends AsyncTask<String, String, Void> {


        @Override
        protected Void doInBackground(String... msgs) {
            Log.e("Replicate task","Activated");

            /******** Send a joining msg from here **************************/
            try {
                String port = msgs[1];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
                String msgToSend = msgs[0];
                BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                msgSend.write(msgToSend);
                msgSend.newLine();
                msgSend.flush();
                socket.close();
            } catch (Exception e) {
                Log.e("Replicate-2 Fail Exception Caught", e.toString());
            }

            /*******************Sending Join block ends here************************/

            return null;
        }
    }
    private class deleteAll extends AsyncTask<String, String, Void> {


        @Override
        protected Void doInBackground(String... msgs) {
            Log.e("Replicate task","Activated");

            for(int i=0;i<ports.length;i++) {

                if(ports[i].equals(myPort))
                    continue;;

                /******** Send a joining msg from here **************************/
                try {
                    String port = ports[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    String msgToSend = "DELETEALL";
                    BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    msgSend.write(msgToSend);
                    msgSend.newLine();
                    msgSend.flush();
                    socket.close();
                } catch (Exception e) {
                    Log.e("Replicate-2 Fail Exception Caught", e.toString());
                }
            }

            /*******************Sending Join block ends here************************/

            return null;
        }
    }



    private class forwarder extends AsyncTask<String, String, Void> {

        /**This here takes arguments the forwarding msg and who to forward to*/

        @Override
        protected Void doInBackground(String... msgs) {


            String replicateMsg = msgs[0];
            String port = hashToPort.get(origSuc);

            try {


                Log.e("Replicatiion-1 key", port);
                String msgToSend = replicateMsg;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
                BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                msgSend.write(msgToSend);
                msgSend.newLine();
                msgSend.flush();
                BufferedReader newReader = new BufferedReader((new InputStreamReader(socket.getInputStream())));
                String starResult = newReader.readLine();
                Log.e("Insert send", starResult);
                socket.close();

            } catch (Exception e) {
                Log.e("Error Replicating key", e.toString());
                Log.e("Sucessor before",mySuc);

                if(failed==false) {
                    blockAll = true;
                    failed = true;
                    failedPort = port;
                    blockAll = false;
                }

            }


                try {

                    String portNew = hashToPort.get(origSuc2);
                    Log.e("Replica-2 key", portNew);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(portNew));
                    BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    msgSend.write(replicateMsg);
                    msgSend.newLine();
                    msgSend.flush();
                    BufferedReader newReader = new BufferedReader((new InputStreamReader(socket.getInputStream())));
                    String starResult = newReader.readLine();
                    Log.e("Insert send", starResult);
                    socket.close();

                } catch (Exception e) {
                    Log.e("Error Replcating-2 Key", e.toString());
                    if(failed==false) {
                        blockAll = true;
                        failed = true;
                        failedPort = port;
                        blockAll = false;
                    }
                }


            /******** Send a Forward msg from here **************************/



            /*******************Sending Forward block ends here************************/

            return null;
        }

        protected void onProgressUpdate(String... strings) {
            String strReceived = strings[0];

            String[] failedArray = strReceived.split("\\s+");
            String sendThis = strReceived;
                try {
                    new forwarder().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, sendThis,hashToPort.get(mySuc) );
                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            return;
        }
    }

}
