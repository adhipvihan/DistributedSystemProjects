package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */

/* Kya kya implement karna hai:
 * Compare method sahi karna hai Arraylist ka --CHECL
 * algo task call karvani hai FIFO ke andar se --CHECK
 * add all AVDS into hashmaphi
 * Content Resolver se add karvana hai algo task me--CHECK!!
*/
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String[] ports = {"11108", "11112", "11116", "11120", "11124"};
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static int counter = -1;
    private ContentResolver mContentResolver;
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
    static final int SERVER_PORT = 10000;
    private static int expecting=0;
    private static HashMap<String,String> tracker;
    public static boolean firstStart=true;
    public static Socket[] outputSockets = new Socket[5];
    private static HashMap<String,Integer> fifoCheck;
    private static HashMap<String,Integer> avd;
    private static int keyCounter=0;
    private static String temp;
    private static int myCounter=0;
    private static int deliverCounter=-1;
    private static HashMap<String,ArrayList<Float>> proposalCheck;
    private static ArrayList<msg> msgTrack;
    private String myPort;

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        tracker = new HashMap<String,String>();
        fifoCheck = new HashMap<String,Integer>();
        avd = new HashMap<String, Integer>();
        avd.put("11108",1);
        avd.put("11112",2);
        avd.put("11116",3);
        avd.put("11120",4);
        avd.put("11124",5);

        setContentView(R.layout.activity_group_messenger);
        mContentResolver = getContentResolver();
        proposalCheck = new HashMap<String, ArrayList<Float>>();
        msgTrack = new ArrayList<msg>();

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }


        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));


        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button button = (Button) findViewById(R.id.button4);

        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
                //TextView localTextView = (TextView) findViewById(R.id.textView1);
                //localTextView.append("\t" + msg);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
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
                    if(incomingMessage.contains("BROADCAST")) {
                        String[] tempMsg = incomingMessage.split("\\s+");
                        incomingMessage = tempMsg[1] + " " + tempMsg[2] + " " + tempMsg[3] + " " + tempMsg[0];
                        publishProgress(incomingMessage);
                    }
                    if(incomingMessage.contains("AGREED") || incomingMessage.contains("REPLY")) {
                        Log.e("Reply Msg", incomingMessage);
                        publishProgress(incomingMessage);
                    }
                    //My Algorithm for FIFO//
//                    try {
//
//                        String res[] = incomingMessage.split("\\s+");
//                        String sendersPort = res[0];
//                        if (!fifoCheck.containsKey(sendersPort)) {
//                            fifoCheck.put(sendersPort, 0);
//                        }
//                        if(incomingMessage.contains("BROADCAST")) {
//                        //Log.e("andar","aagya");
//                        int rcvdNumber = Integer.parseInt(res[1]);
//
//                        if (rcvdNumber != (fifoCheck.get(sendersPort) + 1)) {
//                            String temp = sendersPort + res[1];
//                            //Store it till it is appropriate to insert
//                            tracker.put(temp, incomingMessage);
//                        }
//
//                        if (rcvdNumber == (fifoCheck.get(sendersPort) + 1)) {
//
//                            // Send to Total Ordering Algorithm "incomingmsg" call myalgotask
////                            ContentValues cvObject = new ContentValues();
////                            String count = String.valueOf(fifoCheck.get(sendersPort) + 1);
////                            cvObject.put(KEY_FIELD, count);
////                            cvObject.put(VALUE_FIELD, res[2]);
////                            testInsert(cvObject);
//
//                            //Sending to algo task
//                            publishProgress(incomingMessage);
//
//                            //Update the value
//                            fifoCheck.put(sendersPort, fifoCheck.get(sendersPort) + 1);
//                        }
//
//                        int tempCount = fifoCheck.get(sendersPort) + 1;
//                        temp = sendersPort + String.valueOf(tempCount);
//
//                        if (tracker.containsKey(temp)) {
//
//                            // Checking the case where the key arrived early
//                            while (true) {
//                                if (!tracker.containsKey(temp))
//                                    break;
//                                else {
//                                    String m = tracker.get(temp);
//                                    tracker.remove(temp);
//
//                                    // Send it "m" to TOAlgorithm by calling MyAlgoTask
//                                    publishProgress(m);
//
////                                    ContentValues cvObject = new ContentValues();
////                                    String count = String.valueOf(fifoCheck.get(sendersPort) + 1);
////                                    String[] tempArray = m.split("\\s+");
////                                    cvObject.put(KEY_FIELD, count);
////                                    cvObject.put(VALUE_FIELD, tempArray[2]);
////                                    testInsert(cvObject);
//
//
//                                    //Update Expecting
//                                    fifoCheck.put(sendersPort, fifoCheck.get(sendersPort) + 1);
//                                    //Check for next
//                                    temp = sendersPort + String.valueOf(fifoCheck.get(sendersPort) + 1);
//                                }
//                            }
//                        }
//                    }
//                    }
//                    catch (Exception e) {
//                        Log.e(TAG, e.toString());
//                    }
                    newReader.close();
                   incomingConnection.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            } while (true);
        }

        private boolean testInsert(ContentValues cv) {
            try {
                mContentResolver.insert(mUri, cv);
            } catch (Exception e) {
                Log.e(TAG, e.toString());
                return false;
            }

            return true;
        }

        protected void onProgressUpdate(String... strings) {
            String strReceived = strings[0];
            //String strReceived2 = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
          //  String string = strReceived + "\n";
            try {
                new myAlgoTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strReceived);
            } catch (Exception e) {
                Log.e(TAG, e.toString());
            }
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Log.e("My port",msgs[1]);
            int copyToBeSent = ++myCounter;
            msg toInsertTemp = new msg(myPort, copyToBeSent, msgs[0], false,copyToBeSent);
            msgTrack.add(toInsertTemp);
            String formulate = copyToBeSent + "." + avd.get(myPort);
            Float tempMsg = Float.parseFloat(formulate);
            ArrayList<Float> tempArrayList = new ArrayList<Float>();
            tempArrayList.add(tempMsg);
            proposalCheck.put(msgs[0], tempArrayList);

                for (int i = 0; i < ports.length; i++) {
                    String port = ports[i];
                    try {
                        Socket newsocket= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));
                        String msgToSend;
                        msgToSend ="BROADCAST"+ " " + String.valueOf(msgs[1]) + " " + String.valueOf(copyToBeSent) + " " + msgs[0];
                        if(i==0){
                            String valueTemp = String.valueOf(copyToBeSent)+"."+String.valueOf(avd.get(myPort));
                            ArrayList<Float> tempList = new ArrayList<Float>();
                            Float temp2 = Float.parseFloat(valueTemp);
                            tempList.add(temp2);
                            Log.e("mypriority",valueTemp);
                            proposalCheck.put(String.valueOf(copyToBeSent),tempList);

                        }
                        BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(newsocket.getOutputStream()));
                        msgSend.write(msgToSend);
                        msgSend.flush();
                    }catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException");
                    }
                }
            copyToBeSent=0;

            return null;
        }
    }

    class msg{
        String msg;
        int msgNumber;
        String sender;
        boolean deliver;
        float priority;

                public msg(String _sender,int _msgNumber,String _msg,boolean _deliver,float _priority){
                    this.msg=_msg;
                    this.msgNumber=_msgNumber;
                    this.sender=_sender;
                    this.deliver=_deliver;
                    this.priority=_priority;
                }
    }

    private class myAlgoTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs) {

          String incomingMessage=msgs[0];

            if (incomingMessage.contains("BROADCAST")) {
                String[] incomingMessageTemp = msgs[0].split("\\s+");

                String sendersPort = incomingMessageTemp[0];
                String messageNumber = incomingMessageTemp[1];
                String msgText = incomingMessageTemp[2];

                //CODE TO INSERT INTO PRIORITY QUE!!!!!!!!!!!!

                if(!sendersPort.equalsIgnoreCase(myPort)) {
                    msg toInsert = new msg(sendersPort, Integer.parseInt(messageNumber), msgText, false, ++myCounter);
                    msgTrack.add(toInsert);
                    Collections.sort(msgTrack, new Comparator<msg>() {

                        public int compare(msg e1, msg e2) {

                            return Float.compare(e1.priority, e2.priority);

                        }
                    });
                }

                //Send Reply to the process

                //*************** Format of REPLY************** //
                /* REPLIERS PORT _SPACE_ msgRepliedTo _SPACE_ PROPOSED NUMBER _SPACE_ REPLY_IDENTIFIER */

                StringBuilder replyMsgTemp = new StringBuilder(String.valueOf(myPort) + " " + messageNumber + " " + String.valueOf(myCounter) + " " + "REPLY");
                String replyMsg = replyMsgTemp.toString();
                if (!sendersPort.equals(myPort)){
                    try {
                        Socket newsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(sendersPort));
                        BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(newsocket.getOutputStream()));
                        msgSend.write(replyMsg);
                        msgSend.flush();
                        newsocket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException");
                    }
            }

            }



            //If the msg is AGREED msg
            if (incomingMessage.contains("AGREED")) {
                /* SENDERS PORT __ MSGNUMBER __ AGREED-VALUE __ AGREED TAG */
                String[] tempS = incomingMessage.split("\\s+");
                String senderTemp = tempS[0];
                String msgNumberTemp = tempS[1];
                for (int k = 0; k < msgTrack.size(); k++) {
                    if (msgTrack.get(k).sender.equalsIgnoreCase(senderTemp) && msgTrack.get(k).msgNumber == Integer.parseInt(msgNumberTemp)) {
                        msg temp = msgTrack.get(k);
                        msgTrack.remove(msgTrack.get(k));
                        //temp.priority = Float.parseFloat(incomingMessage[3]);
                        temp.priority = Float.parseFloat(tempS[2]);
                        temp.deliver = true;
                        msgTrack.add(temp);
                        Collections.sort(msgTrack, new Comparator<msg>() {

                            public int compare(msg e1, msg e2) {

                                return Float.compare(e1.priority, e2.priority);

                            }
                        });
                    }
                }
                if (!msgTrack.isEmpty()&&msgTrack.get(0).deliver == true) {
                    while (true) {
                        if (!msgTrack.isEmpty()&&msgTrack.get(0).deliver == true) {
                            ContentValues cvObject = new ContentValues();
                            String count = String.valueOf(++deliverCounter);
                            cvObject.put(KEY_FIELD, count);
                            cvObject.put(VALUE_FIELD, msgTrack.get(0).msg);
                            testInsert(cvObject);
                            msgTrack.remove(0);
                            Collections.sort(msgTrack, new Comparator<msg>() {

                                public int compare(msg e1, msg e2) {

                                    return Float.compare(e1.priority, e2.priority);

                                }
                            });
                        } else
                            break;
                    }
                    //run loop to deliver all TRUE delivery tags
                    //DELIVER IT TO CONTENT RESOLVER
                    // Remove from the front of que
                }

            }

            // If the msg is a reply message //
            if (incomingMessage.contains("REPLY")) {

                String[] tempArray = incomingMessage.split("\\s+");
                String proposal = tempArray[2];
                String msgForWhichProposal = tempArray[1];
                String senderPortNo = tempArray[0];
                //int proposalRecvd = Integer.parseInt(incomingMessage[3]);
                String floatProposal = proposal + "." + avd.get(senderPortNo);
                //  YHA DIKKAT LAG RHI HAI //
                ArrayList<Float> proposals = new ArrayList<Float>();
                if(proposalCheck.get(msgForWhichProposal)!=null) {
                    proposals = proposalCheck.get(msgForWhichProposal);
                    proposalCheck.remove(proposalCheck.get(msgForWhichProposal));
                    Float temp2 = Float.parseFloat(floatProposal);
                    proposals.add(temp2);
                    proposalCheck.put(msgForWhichProposal, proposals);
                }
                if(proposalCheck.get(msgForWhichProposal)==null){
                    Log.e("Never","Execute");
                    Float temp2 = Float.parseFloat(floatProposal);
                    proposals.add(temp2);
                    proposalCheck.put(msgForWhichProposal, proposals);
                }
                // Now broadcast it to all if ArrayList equals size 5 i.e all proposals recived
                ArrayList<Float> checkSize = proposalCheck.get(msgForWhichProposal);
                Log.e("que size",String.valueOf(checkSize.size()));

                if (checkSize.size() == 5) {

                    Float max = Float.MIN_VALUE;
                    for (int i = 0; i < checkSize.size(); i++) {
                        Log.e("Proposal",String.valueOf(checkSize.get(i)));
                        if (checkSize.get(i) > max) {
                            max = checkSize.get(i);
                        }
                    }
                        //Update the priority in Priority Que
                        for (int k = 0; k < msgTrack.size(); k++) {
                            if (msgTrack.get(k).sender.equalsIgnoreCase(myPort) && msgTrack.get(k).msg.equalsIgnoreCase(msgForWhichProposal)) {
                                msg temp = msgTrack.get(k);
                                msgTrack.remove(msgTrack.get(k));
                                temp.priority = max;
                                msgTrack.add(temp);
                                //Log.e("Sort ke phle2", String.valueOf(msgTrack.get(0).priority));
                                Collections.sort(msgTrack, new Comparator<msg>() {

                                    public int compare(msg e1, msg e2) {

                                        return Float.compare(e1.priority, e2.priority);

                                    }
                                });
                                //Log.e("Sort ke baad2", String.valueOf(msgTrack.get(0).priority));
                            }
                        }
                        if (!msgTrack.isEmpty()&&msgTrack.get(0).deliver == true) {
                            while (true) {
                                if (!msgTrack.isEmpty()&&msgTrack.get(0).deliver == true) {
                                    ContentValues cvObject = new ContentValues();
                                    String count = String.valueOf(++deliverCounter);
                                    cvObject.put(KEY_FIELD, count);
                                    cvObject.put(VALUE_FIELD, msgTrack.get(0).msg);
                                    testInsert(cvObject);
                                    msgTrack.remove(0);
                                    Collections.sort(msgTrack, new Comparator<msg>() {

                                        public int compare(msg e1, msg e2) {

                                            return Float.compare(e1.priority, e2.priority);

                                        }
                                    });
                                } else
                                    break;
                            }
                        }

                            /* RE-MULTICAST MSG TO ALL */
                        /******************** MSG FORMAT ************/
                        /* SENDERS PORT __ MSGNUMBER __ AGREED-VALUE __ AGREED TAG */
                        Log.e("Agreed value",String.valueOf(max));

                        String formulate = myPort + " " + msgForWhichProposal + " " + String.valueOf(max) + " " + "AGREED";
                        for (int j = 0; j < ports.length; j++) {
                            String port = ports[j];
                            try {
                                Socket newsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(port));
                                BufferedWriter msgSend = new BufferedWriter(new OutputStreamWriter(newsocket.getOutputStream()));
                                msgSend.write(formulate);
                                msgSend.flush();
                                newsocket.close();
                            } catch (UnknownHostException e) {
                                Log.e(TAG, "ClientTask UnknownHostException");
                            } catch (IOException e) {
                                Log.e(TAG, "ClientTask socket IOException");
                            }
                        }

                    }
                }
            return null;
        }


        private boolean testInsert(ContentValues cv) {
            try {
                mContentResolver.insert(mUri, cv);
            } catch (Exception e) {
                Log.e(TAG, e.toString());
                return false;
            }

            return true;
        }
    }

    }

