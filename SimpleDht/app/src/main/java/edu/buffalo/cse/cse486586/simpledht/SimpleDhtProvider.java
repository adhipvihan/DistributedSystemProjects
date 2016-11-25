package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
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
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;

public class SimpleDhtProvider extends ContentProvider {

    private static final String KEY_FIELD = "key";
    private static int myPosition;
    private static int myPred;
    private static int mySuc;
    private static String myHash;
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    private static final String VALUE_FIELD = "value";
    final String[] ports = {"11124", "11112", "11108", "11116", "11120"};
    private static HashMap<String,String> hashToPort = new HashMap<String, String>();
    static final Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
    private final Uri mUri2 = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    private String myPort;
    static final int SERVER_PORT = 10000;
    private static boolean[] checker = new boolean[5];
    private static HashMap<String,Integer> positionChecker = new HashMap<String, Integer>();
    private static HashMap<String,String> numberToPort = new HashMap<String, String>();
    private ContentResolver xContentResolver;
    private static boolean gotR = false;
    private static boolean insertNow = false;
    private static boolean findHere = false;
    private static boolean delAll = false;
    private static boolean delHere = false;
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private final String[] sortedHash = {
            "177ccecaec32c54b82d5aaafc18a2dadb753e3b1",
            "208f7f72b198dadd244e61801abe1ec3a4857bc9",
            "33d6357cfaaf0f72991b0ecd8c56da066613c089",
            "abf0fd8db03e5ecb199a9b82929e9db79b909643",
            "c25ddd596aa7c81fa12378fa725f706d54325d12"
    };

    public class GroupMessengerDatabase extends SQLiteOpenHelper {

        public static final String DATABASE_NAME = "GroupMessenger.db";
        public static final int DATABASE_VERSION = 2;
        public static final String TABLE_NAME = "gmtable";
        private final Context gcontext;

        GroupMessengerDatabase(Context context){
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
            gcontext=context;
        }

        public void onCreate(SQLiteDatabase db) {
            db.execSQL(" CREATE TABLE " + TABLE_NAME +
                    " (key STRING PRIMARY KEY, " +
                    " value TEXT NOT NULL);");
            db.execSQL("delete from " + TABLE_NAME);
        }

        public void onUpgrade(SQLiteDatabase db,int oldVer,int newVer){
            int version = oldVer;
            if(version==1)
                version=2;
            if(version!=DATABASE_VERSION){
                db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
                onCreate(db);
            }
        }
    }

    private GroupMessengerDatabase newDatabase;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
        String hashedKey = "";
        String predKey = hashToPort.get(String.valueOf(myPred));

        try {
            hashedKey = genHash(selection);
        } catch (Exception e) {
            Log.e("Err Hashing", e.toString());
        }

        if ( (myPosition == myPred && myPosition == mySuc) || delHere==true ) {
            delHere=false;
            if (selection.equalsIgnoreCase("*")) {
                SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
                newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
                myDB.rawQuery("DELETE FROM gmtable", null).moveToFirst();
                return 1;
            } else if (selection.equalsIgnoreCase("@")) {
                SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
                newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
                myDB.rawQuery("DELETE FROM gmtable", null).moveToFirst();
                return 1;
            }
            else{
                SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
                newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
                myDB.rawQuery("DELETE FROM gmtable WHERE key = ?", new String[]{selection}).moveToFirst();
                return 1;
            }

        }

        else{

        if (delAll == true) {
            delAll = false;
            SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
            newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
            myDB.rawQuery("DELETE FROM gmtable", null).moveToFirst();
            return 1;
        }
        else {

            if (selection.equalsIgnoreCase("*")) {
            /*Send a delete msg to all the avds and also delete your storage */

                String delMsg = "DELETEALL" + " " + myPort;
                try {
                    new keySender().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, delMsg);
                } catch (Exception e) {
                    Log.e("Error evoking DELETEALL", e.toString());
                }


                SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
                newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
                myDB.rawQuery("DELETE FROM gmtable", null).moveToFirst();
                return 1;
            } else if (selection.equalsIgnoreCase("@")) {
                SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
                newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
                myDB.rawQuery("DELETE FROM gmtable", null).moveToFirst();
                return 1;
            }
            else {

            /*Check if the delete request can be served by you*/
                if ((predKey.compareTo(myHash) >= 1 && hashedKey.compareTo(predKey) >= 1) || ((predKey.compareTo(myHash) >= 1 && hashedKey.compareTo(myHash) <= 0)) || (hashedKey.compareTo(myHash) <= 0 && hashedKey.compareTo(predKey) >= 1)) {
                    SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
                    newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
                    myDB.rawQuery("DELETE FROM gmtable WHERE key = ?", new String[]{selection}).moveToFirst();
                    return 1;
                } else {
                /*Forward this key to sucessor to delete*/
                    String delKey = "DELETEKEY" + " " + selection + " " + myPort;
                    try {
                        new keySender().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, delKey);
                    } catch (Exception e) {
                        Log.e("Error DELETE-KEY", e.toString());
                    }
                    return 1;
                }

            /*If unable to serve forward it*/
            }

        }
    }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = (String)values.get(KEY_FIELD);
        String value = (String) values.get(VALUE_FIELD);
        String hashedKey="";
        String predKey = hashToPort.get(String.valueOf(myPred));

        try{
            hashedKey = genHash(key);
        }catch(Exception e){
            Log.e("Err Hashing",e.toString());
        }

        if(myPosition==mySuc && myPosition==myPred){
            ContentValues cv = new ContentValues();
            cv.put(KEY_FIELD, key);
            cv.put(VALUE_FIELD, value);
            final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
            long recordNumber = myDB.insert(GroupMessengerDatabase.TABLE_NAME, null, cv);
            Log.v("insert", cv.toString());
            return CONTENT_URI.buildUpon().appendEncodedPath(String.valueOf(recordNumber)).build();
        }
        else if(gotR==true){
            gotR=false;
            ContentValues cv = new ContentValues();
            cv.put(KEY_FIELD, key);
            cv.put(VALUE_FIELD, value);
            final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
            long recordNumber = myDB.insert(GroupMessengerDatabase.TABLE_NAME, null, cv);
            Log.v("insert", cv.toString());
            insertNow=true;
            return CONTENT_URI.buildUpon().appendEncodedPath(String.valueOf(recordNumber)).build();
        }

        else{
            Log.e("My psition",String.valueOf(myPosition));
            if( (predKey.compareTo(myHash)>=1 && hashedKey.compareTo(predKey) >=1) ||((predKey.compareTo(myHash)>=1 && hashedKey.compareTo(myHash) <=0)) ){

                /* Compare your hash with your predecessor
                 If it is bigger than your hash then you are position one
                 Now check if the hash is greater than that predecessor
                 and greater than you too, Then add it
                   */


                ContentValues cv = new ContentValues();
                cv.put(KEY_FIELD, key);
                cv.put(VALUE_FIELD, value);
                final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
                long recordNumber = myDB.insert(GroupMessengerDatabase.TABLE_NAME, null, cv);
                Log.v("insert", cv.toString());
                return CONTENT_URI.buildUpon().appendEncodedPath(String.valueOf(recordNumber)).build();
            }

            else{

                if(hashedKey.compareTo(myHash)<=0 && hashedKey.compareTo(predKey)>=1){
                    ContentValues cv = new ContentValues();
                    cv.put(KEY_FIELD, key);
                    cv.put(VALUE_FIELD, value);
                    final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
                    long recordNumber = myDB.insert(GroupMessengerDatabase.TABLE_NAME, null, cv);
                    Log.v("insert", cv.toString());
                    return CONTENT_URI.buildUpon().appendEncodedPath(String.valueOf(recordNumber)).build();
                }
                else{
                    String senderFormat = key + " " + value + " " + "FORWARD";
                    try {
                        new keySender().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, senderFormat);
                    } catch (Exception e) {
                        Log.e("Error evoking FORWARD", e.toString());
                    }
                    return null;
                }
            }


        }
    }

    @Override
    public boolean onCreate() {
        newDatabase = new GroupMessengerDatabase(getContext());
        hashToPort.put("11108","33d6357cfaaf0f72991b0ecd8c56da066613c089");
        hashToPort.put("11116","abf0fd8db03e5ecb199a9b82929e9db79b909643");
        hashToPort.put("11112","208f7f72b198dadd244e61801abe1ec3a4857bc9");
        hashToPort.put("11124","177ccecaec32c54b82d5aaafc18a2dadb753e3b1");
        hashToPort.put("11120","c25ddd596aa7c81fa12378fa725f706d54325d12");

        xContentResolver = getContext().getContentResolver();

        positionChecker.put("11108",2);
        positionChecker.put("11116",3);
        positionChecker.put("11112",1);
        positionChecker.put("11124",0);
        positionChecker.put("11120",4);

        numberToPort.put("0","11124");
        numberToPort.put("1","11112");
        numberToPort.put("2","11108");
        numberToPort.put("3","11116");
        numberToPort.put("4","11120");

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        myPosition = positionChecker.get(myPort);
        mySuc = positionChecker.get(myPort);
        myPred = positionChecker.get(myPort);
        myHash = hashToPort.get(myPort);
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }



        // If the position is Zero then set predecessor as position 4
        // also if the position is 4 then set the sucessor as position 1

        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, null, myPort);
        // after this send a request to the starter node 5554 with JOIN msg


        // Now deploy a server task here


        // If server recieves a msg JOIN from itself then check a flag and
        // insert all the values locally



        //otherwise compare the values and send them accordingly





        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        String temp = selection;
        if( (mySuc==myPosition && myPred==myPosition)){

            if (temp.equals("*")) {
                //try{temp = genHash(selection);}catch (Exception e){Log.e("Hashing Query Error",e.toString());}
                final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
                SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
                newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
                Cursor queryCursor = myDB.rawQuery("SELECT * FROM gmtable", null);
                Log.v("query", "1");
                Log.v("query", selection);
                return queryCursor;
            } else if (temp.equals("@")) {
                //try{temp = genHash(selection);}catch (Exception e){Log.e("Hashing Query Error",e.toString());}
                final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
                SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
                newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
                Cursor queryCursor = myDB.rawQuery("SELECT * FROM gmtable", null);
                Log.v("query", "2");
                Log.v("query", selection);
                return queryCursor;
            }

            else {

                final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
                SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
                newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
                Cursor queryCursor = myDB.rawQuery("SELECT key,value FROM gmtable WHERE key = ?", new String[]{temp});
                Log.v("query", "3");
                Log.v("query", selection);
                Log.e("Returned this",queryCursor.toString());
                return queryCursor;
            }
        }

    else{
        String predKey = hashToPort.get(String.valueOf(myPred));
        Log.e("pred key", predKey);

        if (temp.equals("*")) {
            //try{temp = genHash(selection);}catch (Exception e){Log.e("Hashing Query Error",e.toString());}
            final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
            SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
            newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
            Cursor queryCursor = myDB.rawQuery("SELECT * FROM gmtable", null);
            Log.v("query", "4");
            Log.v("query", selection);
            return queryCursor;
        } else if (temp.equals("@")) {
            //try{temp = genHash(selection);}catch (Exception e){Log.e("Hashing Query Error",e.toString());}
            final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
            SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
            newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
            Cursor queryCursor = myDB.rawQuery("SELECT * FROM gmtable", null);
            Log.v("query", "5");
            Log.v("query", selection);
            return queryCursor;
        }

        else {
            String hashedReqKey = "";

            try {
                hashedReqKey = genHash(temp);
            } catch (Exception e) {
                Log.e("Err Hashing", e.toString());
            }

            if ((predKey.compareTo(myHash) >= 1 && hashedReqKey.compareTo(predKey) >= 1) || ((predKey.compareTo(myHash) >= 1 && hashedReqKey.compareTo(myHash) <= 0)) || (hashedReqKey.compareTo(myHash)<=0 && hashedReqKey.compareTo(predKey)>=1) ) {
                final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
                SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
                newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
                Cursor queryCursor = myDB.rawQuery("SELECT key,value FROM gmtable WHERE key = ?", new String[]{selection});
                Log.v("query", "6");
                Log.v("query", selection);
                return queryCursor;
            }

            else {
                String reqSend = "REQUEST" + " " + temp + " " + myPort;
                Log.e("Sending Req",reqSend);
                try {
                    new keySender().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, reqSend);
                } catch (Exception e) {
                    Log.e("Error evoking Request", e.toString());
                }


                    while(insertNow!=true){

                    }
                gotR=false;
                insertNow=false;
//                try {
//                    Thread.sleep(3000);
//                }catch (Exception e){
//                    Log.e("Sleep error",e.toString());
//                }

                final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
                SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
                newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
                Cursor queryCursor = myDB.rawQuery("SELECT key,value FROM gmtable WHERE key = ?", new String[]{temp});
                Log.v("query", "7");
                Log.v("query", selection);
                return queryCursor;
            }


        }
    }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                String port = ports[2];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
                String msgToSend =myPort + " " + "JOIN";
                BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                msgSend.write(msgToSend);
                msgSend.flush();
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }catch(Exception e){
                Log.e("Client-task","Exception");
            }

            return null;
        }
    }

    //AsyncTask For Server
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];


            do {
                try {

                    Socket incomingConnection = serverSocket.accept();

                    BufferedReader newReader = new BufferedReader(new InputStreamReader(incomingConnection.getInputStream()));
                    String incomingMessage = newReader.readLine();
//                    //publishProgress(incomingMessage);

                    if(incomingMessage.contains("DELETEKEY")){
                        Log.e("Delete Req",incomingMessage);
                        String[] delKmsg  = incomingMessage.split("\\s+");
                        String hashedKey = "";
                        String predKey = hashToPort.get(String.valueOf(myPred));

                        try{
                            hashedKey = genHash(delKmsg[1]);
                        }catch(Exception e){
                            Log.e("Err Hashing",e.toString());
                        }
                        /*Check if key is present. Then delete*/
                        if( (predKey.compareTo(myHash)>=1 && hashedKey.compareTo(predKey) >=1) ||((predKey.compareTo(myHash)>=1 && hashedKey.compareTo(myHash) <=0)) || (hashedKey.compareTo(myHash)<=0 && hashedKey.compareTo(predKey)>=1) ) {
                            delHere=true;
                            xContentResolver.delete(mUri2,delKmsg[1],null);

                        }

                        /*forward it. It doesn't belongs here*/

                        else{
                            publishProgress(incomingMessage);
                        }

                        }

                    if(incomingMessage.contains("DELETEALL")){
                        delAll=true;

                        //Delete all keys from avd and also forward the msg
                        // Also remember to check that the msg isn't from you
                        // If it is then dont forward it

                        String[] dMsg = incomingMessage.split("\\s+");

                        if(!dMsg[1].equalsIgnoreCase(myPort)){
                            /* Forward it*/
                            publishProgress(incomingMessage);

                        }
                        xContentResolver.delete(mUri2,"*",null);

                    }

                    if(incomingMessage.contains("REQUEST")){
                        String[] reqA = incomingMessage.split("\\s+");
                        String keyR = reqA[1];
                        String hashedKey="";
                        String requestor = reqA[2];
                        String predKey = hashToPort.get(String.valueOf(myPred));

                        try{
                            hashedKey = genHash(keyR);
                        }catch(Exception e){
                            Log.e("Err Hashing",e.toString());
                        }

                        /*Check if the key is present then query*/
                        if( (predKey.compareTo(myHash)>=1 && hashedKey.compareTo(predKey) >=1) ||((predKey.compareTo(myHash)>=1 && hashedKey.compareTo(myHash) <=0)) || (hashedKey.compareTo(myHash)<=0 && hashedKey.compareTo(predKey)>=1) ) {
                            // Find the key and return it to the Requester
                            findHere=true;
                            Cursor resultCursor = xContentResolver.query(mUri2, null,
                                    keyR, null, null);
                            Log.e("GA THIS",resultCursor.toString());

                            if (resultCursor == null) {
                                Log.e(TAG, "Result null");
                                throw new Exception();
                            }

                            int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                            int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                            if (keyIndex == -1 || valueIndex == -1) {
                                Log.e(TAG, "Wrong columns");
                                resultCursor.close();
                                throw new Exception();
                            }

                            resultCursor.moveToFirst();

                            if (!(resultCursor.isFirst() && resultCursor.isLast())) {
                                Log.e(TAG, "Wrong number of rows");
                                resultCursor.close();
                                throw new Exception();
                            }

                            String returnKey = resultCursor.getString(keyIndex);
                            String returnValue = resultCursor.getString(valueIndex);

                            resultCursor.close();
                            Log.e("THIS"+returnKey,returnValue);

                            String value="";
                            String reqReply = "REQSERVICE"+" "+keyR+" "+ returnValue+" "+requestor;
                            publishProgress(reqReply);
                            }

                        else{
                                //Forward the msg
                                publishProgress(incomingMessage);
                            }
                        }

                    if(incomingMessage.contains("REQSERVICE")){
                        Log.e("Recvd VALUE",incomingMessage);
                        String[] rs = incomingMessage.split("\\s+");
                        // Set boolean flag gotReply to true
                        gotR=true;

                        //Put it into ContentResolver

                        ContentValues cvObject = new ContentValues();
                        cvObject.put(KEY_FIELD, rs[1]);
                        cvObject.put(VALUE_FIELD, rs[2]);
                        testInsert(cvObject);
                    }

                    if(incomingMessage.contains("FORWARD")){
                        String[] forwardArray = incomingMessage.split("\\s+");
                        String fokey = forwardArray[0];
                       // Log.e("Recvd Forward",fokey);
                        String fovalue = forwardArray[1];
                        ContentValues cvObject = new ContentValues();
                        cvObject.put(KEY_FIELD, fokey);
                        cvObject.put(VALUE_FIELD, fovalue);
                        testInsert(cvObject);
                    }

                    if(incomingMessage.contains("SUC")){
                        String[] predArray = incomingMessage.split("\\s+");
                        mySuc = Integer.parseInt(predArray[1]);
                        Log.e("My Info",String.valueOf(positionChecker.get(String.valueOf(myPred)))+" "+String.valueOf(positionChecker.get(String.valueOf(mySuc))));
                    }
                    if(incomingMessage.contains("PRED")){
                        String[] sucArray = incomingMessage.split("\\s+");
                        myPred = Integer.parseInt(sucArray[1]);
                        Log.e("My Info",String.valueOf(positionChecker.get(String.valueOf(myPred)))+" "+mySuc);

                    }
                    if(incomingMessage.contains("UPDATE") && incomingMessage.contains(myPort)){
                        Log.e("Update Rcvd",incomingMessage);
                        String[] updateArray = incomingMessage.split("\\s+");
                        /*update the sucessors and predecessors */
                        int xx = Integer.parseInt(myPort);
                        xx=xx/2;
                        Log.e("My port",String.valueOf(xx));
                        Log.e("my position",String.valueOf(myPosition));

                        if(!updateArray[3].equalsIgnoreCase("-1") && !updateArray[2].equalsIgnoreCase("-1")) {
                            mySuc = Integer.parseInt(numberToPort.get(updateArray[3]));
                            myPred = Integer.parseInt(numberToPort.get(updateArray[2]));
                            Log.e("My Info",String.valueOf(positionChecker.get(String.valueOf(myPred)))+" "+String.valueOf(positionChecker.get(String.valueOf(mySuc))));

                        }

                    }

                    if(incomingMessage.contains("JOIN")){
                       // Log.e("Recvd Join Request",incomingMessage);
                        /******************* Logic For Determining Sucessor & Predecessor Dynamically *********

                         Set a boolean array "checker"
                         If a request for join arrives. Find out the position of that particular port inside the
                         array. Set it to TRUE.
                         Take two boolean flags. FoundP & FoundS
                         Now find out the predecessor from the boolean array by searching backwards from the key.
                         Now find the sucessor by searching forward from the array.
                         Now based send that value to the Sender along with his port identifier So that he can
                         keep track of the sucessor and predecessor.





                         ***************************************************************************************/

                        boolean foundP=false;
                        boolean foundS = false;
                        int pred=-1;
                        int suc=-1;
                        String[] tempAr = incomingMessage.split("\\s+");

                        int position = positionChecker.get(tempAr[0]);
                       // Log.e("position true",String.valueOf(position));
                        checker[position] = true;

                        for(int i=position;i>=0;i--){
                            if(checker[i]==true && i!=position){
                                pred = i;
                                foundP=true;
                                break;
                            }
                        }
                        if(foundP==false){
                            for(int i=4;i>0;i--){
                                if(checker[i]==true && i!=position){
                                    pred=i;
                                    foundP=true;
                                    break;
                                }
                            }
                        }

                        for(int i=position;i<checker.length;i++){
                            if(checker[i]==true && i!=position){
                                suc=i;
                                foundS=true;
                                break;
                            }
                        }
                        if(foundS==false){
                            for(int i=0;i<checker.length;i++){
                                if(checker[i]==true && i!=position){
                                    suc=i;
                                    break;
                                }
                            }
                        }
                        String replyback = "UPDATE"+" "+tempAr[0]+" "+pred+" "+suc;
                        publishProgress(replyback);
                    }
                    //newReader.close();
                    //incomingConnection.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }while(true);
        }

        private boolean testInsert(ContentValues cv) {
            try {
                xContentResolver.insert(mUri2, cv);
            } catch (Exception e) {
                Log.e(TAG, e.toString());
                return false;
            }

            return true;
        }

        protected void onProgressUpdate(String...strings) {


            String strReceived = strings[0];

            if(strReceived.contains("REQUEST")){
                try {
                    new keySender().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, strings[0]);
                } catch (Exception e) {
                    Log.e("Error evoking FORWARD", e.toString());
                }
            }
            if(strReceived.contains("DELETEKEY")){
                try {
                    new keySender().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, strings[0]);
                } catch (Exception e) {
                    Log.e("Error DELKEY forward", e.toString());
                }
            }

            if(strReceived.contains("DELETEALL")){
                try {
                    new keySender().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, strings[0]);
                } catch (Exception e) {
                    Log.e("Error DELKEY forward", e.toString());
                }
            }

            if(strReceived.contains("REQSERVICE")){
                try {
                    new myAlgoTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, strings[0]);
                } catch (Exception e) {
                    Log.e("Error evoking FORWARD", e.toString());
                }
            }
            if(strings[0].contains("FORWARD")){
                try {
                    new keySender().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, strings[0]);
                } catch (Exception e) {
                    Log.e("Error evoking FORWARD", e.toString());
                }
            }
            if(strReceived.contains("UPDATE")) {
                try {
                    new myAlgoTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, strReceived);
                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }


            //TextView remoteTextView = (TextView) findViewById(R.id.remote_text_display);
            //remoteTextView.append(strReceived + "\t\n");
            //TextView localTextView = (TextView) findViewById(R.id.local_text_display);
            //localTextView.append("\n");
        }
    }


    private class keySender extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs) {

            String[] strReceived = msgs[0].split("\\s+");
            Log.e("Forwarding This",msgs[0]);
           // Log.e("My position",String.valueOf(myPosition));


            try {

                String portforward = String.valueOf(mySuc);
              //  Log.e("forwarding to",portforward);
                Socket sucSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(portforward));
                String forwardmsg = msgs[0];
                BufferedWriter forwardWriter = new BufferedWriter(new OutputStreamWriter(sucSocket.getOutputStream()));
                forwardWriter.write(forwardmsg);
                forwardWriter.flush();
                sucSocket.close();

            } catch (Exception e) {
                Log.e("Error", "Forwarding");
                Log.e("error",e.toString());
            }

            return null;
        }
    }


    private class myAlgoTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs) {



            if(msgs[0].contains("UPDATE")) {

                //Log.e("Executed","Server thread");
                String sr = "";
                String pd = "";
               // Log.e("Update send", msgs[0]);

                String[] strReceived = msgs[0].split("\\s+");
                if (!strReceived[2].equals("-1") && !strReceived[3].equals("-1")) {
                    sr = numberToPort.get(strReceived[3]);
                    pd = numberToPort.get(strReceived[2]);
                }
                // Log.e("Sucessor",sr);
                //  Log.e("Predecessor",pd);

                try {

                    String portemp22 = strReceived[1];
                    Socket socket22 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(portemp22));
                    String msgToSend22 = msgs[0];
                    BufferedWriter msgSend22 = new BufferedWriter(new OutputStreamWriter(socket22.getOutputStream()));
                    msgSend22.write(msgToSend22);
                    msgSend22.flush();
                    socket22.close();

                } catch (Exception e) {
                    Log.e("Error", "Informing Suc Pred");
                    Log.e("error", e.toString());
                }


                if (!sr.equals("") && !pd.equals("")) {
                    try {

                        String portemp = sr;
                        Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portemp));
                        String msgToSend2 = "PRED" + " " + strReceived[1];
                        BufferedWriter msgSend2 = new BufferedWriter(new OutputStreamWriter(socket2.getOutputStream()));
                        msgSend2.write(msgToSend2);
                        msgSend2.flush();
                        socket2.close();
                        // Log.e("Sent",msgToSend2);


                        String portemp3 = pd;
                        Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portemp3));
                        String msgToSend3 = "SUC" + " " + strReceived[1];
                        BufferedWriter msgSend3 = new BufferedWriter(new OutputStreamWriter(socket3.getOutputStream()));
                        msgSend3.write(msgToSend3);
                        msgSend3.flush();
                        socket3.close();
                        //  Log.e("Sent", msgToSend3);

                    } catch (Exception e) {
                        Log.e("Error", "Informing Suc Pred");
                        Log.e("Error", e.toString());
                    }

                }
            }

            if(msgs[0].contains("REQSERVICE")){
                String[] reqReply = msgs[0].split("\\s+");
                try {

                    String portemp = reqReply[3];
                    Socket socket22 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(portemp));
                    String msgToSend22 = msgs[0];
                    BufferedWriter msgSend22 = new BufferedWriter(new OutputStreamWriter(socket22.getOutputStream()));
                    msgSend22.write(msgToSend22);
                    msgSend22.flush();
                    socket22.close();

                } catch (Exception e) {
                    Log.e("Error", "Servicing Request");
                    Log.e("error", e.toString());
                }

            }


            return null;
        }
    }
}
