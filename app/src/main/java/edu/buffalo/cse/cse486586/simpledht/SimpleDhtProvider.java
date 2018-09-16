//https://developer.android.com/reference/android/database/sqlite/SQLiteCursor.html

package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.ArrayList;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.TreeMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    private Uri uribuilder;
    private MyDBHandler myDB;
    private SQLiteDatabase sqlDB;
    String myPort = null;
    String portStr = null;
    String pred = null;
    String succ = null;
    String keyhash = null;
    String porthash = null;
    String predhash = null;
    String selhash = null;
    String mentry = null;
    String values = null;

    class Message
    {
        String k;
        String val;
        String txt;
        String sourcePort;
        String suc;
        String pre;

        public String getK() {
            return k;
        }

        public void setK(String k) {
            this.k = k;
        }

        public String getVal() {
            return val;
        }

        public void setVal(String val) {
            this.val = val;
        }

        public String getTxt() {
            return txt;
        }

        public void setTxt(String txt) {
            this.txt = txt;
        }
        @Override
        public String toString()
        {
         return txt + "$" + k + "$" + val + "$" + sourcePort + "$" + suc + "$" + pre;
        }

        public String getSourcePort() {
            return sourcePort;
        }

        public void setSourcePort(String sourcePort) {
            this.sourcePort = sourcePort;
        }

        public String getSuc() {
            return suc;
        }

        public void setSuc(String success) {
            this.suc = success;
        }

        public String getPre() {
            return pre;
        }

        public void setPre(String predcess) {
            this.pre = predcess;
        }
    }

    TreeMap<String, String> tmap = new TreeMap<String, String>();

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        // TODO Auto-generated method stub
        String key = (String) values.get("key");
        String value = (String) values.get("value");
        Log.v("initial insert", "pred:"+pred);
        if (pred==(null) && succ==(null)) // Only one avd hence local insert
        {
            Log.v("insert one avd", "pred:" + pred + "succ:" + succ);
            myDB.insert(sqlDB, values);
        }
        else {
            try {
                keyhash = genHash(key);
                porthash = genHash(portStr);
                predhash = genHash(pred);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            if ((predhash.compareTo(keyhash) < 0) && (keyhash.compareTo(porthash) <= 0))
            {
                Log.v("INSERT", "Adding key:"+values.get("key")+" at port: "+portStr);
                myDB.insert(sqlDB, values);
            }
            else if ((predhash.compareTo(porthash) > 0) && (((keyhash.compareTo(predhash) > 0) || (keyhash.compareTo(porthash) < 0))))
            {
                Log.v("INSERT", "Adding key:"+values.get("key")+" at port: "+portStr);
                myDB.insert(sqlDB, values);
            }
            else
            {
                Log.v("third else", "pred:" + predhash + "succ:" + succ);
                Message msg = new Message();
                msg.setK(key);
                msg.setVal(value);
                msg.setTxt("Insert");
                Log.v("Insert","not found sending to succ"+succ);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Integer.toString(Integer.parseInt(succ)*2));
            }
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        //Log.v("OnCreate", "inside");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.v("OnCreate","my port no is: "+portStr);
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.v("OnCreate ", "after server socket creation");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            //Log.v("OnCreate", "created server socket");
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket" + e.getMessage());
        }

        Message msg = new Message();
        msg.setSourcePort(portStr);
        msg.setTxt("Join");
        //String msg = "Join" + portStr;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Integer.toString(11108));
        myDB = new MyDBHandler(getContext(), null, null, 1);
        sqlDB = myDB.getWritableDatabase();
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            //Socket clientSocket;
            //Log.v("Server Task", "inside");
            try {
                while (true)
                {
                    Log.v("Server Task ", "just inside server task");
                    Socket clientSocket = serverSocket.accept();
                    //clientSocket.setSoTimeout(2000);
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    //Log.v("Server Task","Within while loop -> Server accepted -> created buffer");
                    String mes = in.readLine();
                    Log.v("Server Task ", "read message from client" + mes);
                    //Log.v("Server Task","Within while loop " + mes);
                    if(mes!=null)
                    {
                        String[] divide = mes.split("\\$");
                        String vers = divide[0];
                        String ke = divide[1];
                        String va = divide[2];
                        String sp = divide[3];
                        String success = divide[4];
                        String predecess = divide[5];
                        if (!vers.equals("Query"))
                        {
                            Log.v("Server Task","vers : "+vers);
                            PrintWriter cpw2 = new PrintWriter(clientSocket.getOutputStream(), true);
                            cpw2.println("check");
                        }
                        if (vers.equals("Insert"))
                        {
                            Log.v("Server Task", "inserted in succ");
                            ContentValues cv = new ContentValues();
                            cv.put("key", ke);
                            cv.put("value", va);
                            insert(null, cv);
                        }
                        //Log.v("Server Task", "key:" + ke + "value:" + va);
                        else if (vers.equals("Join"))
                        {
                            //Log.v("Server Task", "Reached JOIN with sp:" + sp);
                            try {
                                porthash = genHash(sp);
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }
                            tmap.put(porthash, sp);
                            //Log.v("Server Task | Oncreate " + myPort, "put map size: " + tmap.size());
                            Set set = tmap.entrySet();
                            Iterator iterator = set.iterator();
                            //Log.v("Join before setting", "succ:" + succ + "pred:" + pred);
                            if (tmap.size() > 1)
                            {
                                String pred = tmap.firstKey();
                                String succ = "";
                                while (iterator.hasNext())
                                {
                                    Map.Entry mentry = (Map.Entry) iterator.next();
                                    if (mentry.getValue().equals(sp))
                                    {
                                        if (mentry.getKey().equals(tmap.firstKey()))
                                        {
                                            succ = (String) ((Map.Entry) iterator.next()).getValue();
                                            succ = tmap.get(tmap.higherKey((String) mentry.getKey()));
                                            String temp_pred = tmap.lastKey();
                                            pred = tmap.get(temp_pred);
                                            Log.v("In join first entry", "succ:"+ succ + "pred:" + pred + "sp:" + sp);
                                            break;
                                        }
                                        else if (mentry.getKey().equals(tmap.lastKey()))
                                        {
                                            String temp_succ = tmap.firstKey();
                                            succ = tmap.get(temp_succ);
                                            Log.v("In join last entry", "succ:"+ succ + "pred:" + pred + "sp:" + sp);
                                            break;
                                        }
                                        else
                                        {
                                            succ = (String) ((Map.Entry) iterator.next()).getValue();
                                            Log.v("In join normal entry", "succ:"+ succ + "pred:" + pred + "sp:" + sp);
                                            break;
                                        }
                                    }
                                    pred = (String) mentry.getValue();
                                }
                                Log.v("Server Task","JOIN | Dumping tmap");
                                for (Map.Entry<String, String> entry : tmap.entrySet()) {
                                    Log.v("Server Task","JOIN | entry :"+entry.getValue());
                                }
                                //Log.v("Join after setting", "succ:" + succ + "pred:" + pred + "sp:" + sp);
                                Message msg = new Message();
                                msg.setTxt("Update successor");
                                msg.setSuc(sp);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Integer.toString(Integer.parseInt(pred)*2));

                                msg = new Message();
                                msg.setTxt("Update predecessor");
                                msg.setPre(sp);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Integer.toString(Integer.parseInt(succ)*2));

                                msg = new Message();
                                msg.setTxt("Update current node");
                                msg.setPre(pred);
                                msg.setSuc(succ);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Integer.toString(Integer.parseInt(sp)*2));
                            }
                            //Log.v("Server after setting", "succ:" + succ + "pred:" + pred + "sp:" + sp);
                        }
                        else if (vers.equals("Update successor"))
                        {
                            succ = success;
                            Log.v("JOIN|update successor","PRED:"+pred+" CURR:"+portStr+" SUCC:"+succ);
                        }
                        else if (vers.equals("Update predecessor"))
                        {
                            pred = predecess;
                            Log.v("JOIN|update predecessor","PRED:"+pred+" CURR:"+portStr+" SUCC:"+succ);
                        }
                        else if (vers.equals("Update current node"))
                        {
                            succ = success;
                            pred = predecess;
                            Log.v("JOIN|update current ","PRED:"+pred+" CURR:"+portStr+" SUCC:"+succ);
                        }
                        else if(vers.equals("Query"))
                        {
                            Log.v("Server Task", "Calling query for key: "+ke + "to send to port: " + sp);
                            Cursor cursor = query(null,null,ke,new String[]{sp},null,null);
                            Log.v("Server Task","Passing query result back");
                            //Write this cursor into socket
                            JSONArray jsonObjK = new JSONArray();
                            JSONArray jsonObjV = new JSONArray();
                            int i = 0;
                            cursor.moveToFirst();
                            while(!cursor.isAfterLast())
                            {
                                Log.v("Server Task","inside cursor iteration");
                                jsonObjK.put(i, cursor.getString(cursor.getColumnIndex("key")));
                                jsonObjV.put(i, cursor.getString(cursor.getColumnIndex("value")));
                                Log.v("Server Task","Query result key & value extracted" );
                                i++;
                                cursor.moveToNext();
                            }
                            Log.v("Server Task","Query result json key array: "+ jsonObjK.toString() + "json value: "+ jsonObjV.toString() + "extracted");
                            JSONObject msgJson = new JSONObject();
                            msgJson.put("key",jsonObjK);
                            msgJson.put("value",jsonObjV);
                            String msg = msgJson.toString();
                            Log.v("Server Task","Query result putting in string: "+ msg );
                            PrintWriter cpw2 = new PrintWriter(clientSocket.getOutputStream(), true);
                            cpw2.println(msg);
                            //cursor.close();
                            Log.v("Server Task","Query result sent to client via json object"+ msg);
                        }
                        else if(vers.equals("Delete"))
                        {
                            Log.v("Server Task", "Calling delete for key: "+ke + "to send to port: " + sp);
                            delete(null,ke,new String[]{sp});
                            Log.v("Server Task","Passing delete result back");
                        }
                    }
                    //clientSocket.close();
                }
            }catch(IOException e){
                e.printStackTrace();
            }catch (Exception e){
                e.printStackTrace();
            }
            return null;
        }

    }

    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {
            //Log.v("Client Task", "inside");
            try {
                Log.v("Client Task", "my port:" + portStr + " dest port: " + msgs[1]);
                Log.v("Client Task","reach client with message "+msgs[0]);
                String remotePort = msgs[1];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),(Integer.parseInt(remotePort)));
                Log.v("Client Task ", "socket created");
                PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
                cpw2.println(msgs[0]);
                //BufferedWriter write = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                //write.write(msgs[0]);
                Log.v("Client Task ", "passed message to server task: "+ msgs[0]);
                //write.flush();
                //write.close();
                //socket.close();
                Log.v("Client Task","Creating client socket to read");
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String msgReceived = in.readLine();
                Log.v("Client Task","Read Bufferred Reader"+msgReceived);
                return msgReceived;
                }
                catch (UnknownHostException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask UnknownHostException");
                }catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                    Log.e(TAG,e.getMessage());
                }catch (Exception e) {
                    e.printStackTrace();
                }
            return null;
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        try{
            if (pred == (null) && succ == (null))
            {
                if (selection.equals("@") || selection.equals("*"))
                {
                    myDB.deleteAll(sqlDB);
                    return 1;
                }
                else
                {
                    myDB.delete(sqlDB,selection);
                    return 1;
                }
            }
            else
                {
                if (selection.equals("@"))
                {
                    myDB.deleteAll(sqlDB);
                    return 1;
                }
                else if (selection.equals("*"))
                {
                    myDB.deleteAll(sqlDB);
                    String firstQueryPort = "";
                    if(selectionArgs!=null && selectionArgs[0]!=null)
                    {
                        firstQueryPort = selectionArgs[0];
                        if (firstQueryPort.compareTo(succ) == 0)
                        {
                            return 1;
                        }
                        else
                        {
                            Message msg = new Message();
                            msg.setTxt("Delete");
                            msg.setK(selection);
                            msg.setSourcePort(firstQueryPort);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Integer.toString(Integer.parseInt(succ) * 2));
                            return 1;
                        }
                    }
                    else
                    {
                        Message msg = new Message();
                        msg.setTxt("Delete");
                        msg.setK(selection);
                        msg.setSourcePort(portStr);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Integer.toString(Integer.parseInt(succ) * 2));
                        return 1;
                    }
                }
                else
                {
                    Log.v("Delete", "delete for key");
                    try {
                        selhash = genHash(selection);
                        porthash = genHash(portStr);
                        predhash = genHash(pred);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                    if ((predhash.compareTo(selhash) < 0) && (selhash.compareTo(porthash) <= 0))
                    {
                        Log.v("Delete|First If", "pred:" + predhash + "succ:" + succ);
                        myDB.delete(sqlDB, selection);
                        Log.v("Delete|First If", "Found key: "+selection+" at port:" + portStr);
                        return 1;
                    }
                    else if ((predhash.compareTo(porthash) > 0) && (((selhash.compareTo(predhash) > 0) || (selhash.compareTo(porthash) < 0))))
                    {
                        Log.v("Delete|Second If", "pred:" + predhash + "succ:" + succ);
                        myDB.delete(sqlDB, selection);
                        Log.v("Delete|Second If", "Found key: "+selection+" at port:" +  portStr);
                        return 1;
                    }
                    else
                    {
                        Log.v("Delete|Else", "pred:" + predhash + "succ:" + succ);
                        Message msg = new Message();
                        msg.setK(selection);
                        Log.v("Delete", "testing selectionArgs " + selectionArgs);
                        if (selectionArgs != null && selectionArgs[0] != null)
                        {
                            Log.v("Delete", "testing SelectionArgs!=null");
                            Log.v("Delete", "first query port:");
                            String firstQueryPort = selectionArgs[0];
                            msg.setSourcePort(firstQueryPort);
                            Log.v("Delete", "first query port:" + firstQueryPort);
                        }
                        else
                        {
                            Log.v("Delete", "testing SelectionArgs==null");
                            Log.v("Delete", "got from portStr:" + portStr);
                            msg.setSourcePort(portStr);
                        }
                        msg.setTxt("Delete");
                        Log.v("Delete", "not found forwarding" + msg.toString() + "to succ: " + succ);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Integer.toString(Integer.parseInt(succ) * 2));
                        return 1;
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            Log.e("DELETE","exception");
        }
        return 0;
    }



    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        try {
            Cursor cursor = null;
            //String firstQueryPort = selectionArgs[0];
            Log.v("Query", "outside if");
            if (pred == (null) && succ == (null))
            {
                if (selection.equals("@") || selection.equals("*"))
                {
                    cursor = myDB.queryAll(sqlDB, selection);
                    return cursor;
                }
                else
                {
                    cursor = myDB.query(sqlDB, selection);
                    return cursor;
                }
            }
            else
            {
                Log.v("Query", "more than one avd");
                if (selection.equals("@"))
                {
                    cursor = myDB.queryAll(sqlDB, selection);
                    return cursor;
                }
                else if (selection.equals("*"))
                {
                    cursor = myDB.queryAll(sqlDB, selection);
                    String firstQueryPort = "";
                    if(selectionArgs!=null && selectionArgs[0]!=null)
                    {
                        firstQueryPort = selectionArgs[0];
                        if(firstQueryPort.compareTo(succ)==0)
                        {
                            return cursor;
                        }
                        else
                        {
                            Message msg = new Message();
                            Log.v("Query", "* got from portStr:"+ firstQueryPort);
                            msg.setSourcePort(firstQueryPort);
                            msg.setK("*");
                            msg.setTxt("Query");
                            String receivedJsonSTring = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Integer.toString(Integer.parseInt(succ) * 2)).get();
                            Log.v("Query","Query result received from client as : "+receivedJsonSTring);
                            JSONObject json = new JSONObject(receivedJsonSTring);
                            JSONArray jsonK = json.getJSONArray("key");
                            JSONArray jsonV = json.getJSONArray("value");
                            Log.v("Query","Getting jsonKeyArray :"+ jsonK.toString() + "jsonValueArray :"+ jsonV.toString() );
                            MatrixCursor cursorQuery = new MatrixCursor(new String[]{"key", "value"});
                            int i = 0;
                            while (i < jsonK.length())
                            {
                                Log.v("Query","inside MatrixCursor iterator" );
                                cursorQuery.addRow(new Object[]{jsonK.get(i), jsonV.get(i)});
                                Log.v("Query","inside MatrixCursor key: "+ jsonK.get(i).toString()+ "value: "+ jsonV.get(i).toString());
                                i++;
                            }
                            Log.v("Query", "Final query result received key: "+ DatabaseUtils.dumpCursorToString(cursorQuery));
                            cursor.moveToFirst();
                            while (!cursor.isAfterLast())
                            {
                                Object[] values = {cursor.getString(0), cursor.getString(1)};
                                cursorQuery.addRow(values);
                                cursor.moveToNext();
                            }
                            Log.v("Query", "Final query result received key: "+ DatabaseUtils.dumpCursorToString(cursorQuery));
                            return cursorQuery;
                        }
                    }
                    else
                    {
                        Message msg = new Message();
                        Log.v("Query", "* got from portStr:"+ portStr);
                        msg.setTxt("Query");
                        msg.setSourcePort(portStr);
                        msg.setK("*");
                        String receivedJsonSTring = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Integer.toString(Integer.parseInt(succ) * 2)).get();
                        Log.v("Query","Query result received from client as : "+receivedJsonSTring);
                        JSONObject json = new JSONObject(receivedJsonSTring);
                        JSONArray jsonK = json.getJSONArray("key");
                        JSONArray jsonV = json.getJSONArray("value");
                        Log.v("Query","Getting jsonKeyArray :"+ jsonK.toString() + "jsonValueArray :"+ jsonV.toString() );
                        MatrixCursor cursorQuery = new MatrixCursor(new String[]{"key", "value"});
                        int i = 0;
                        while (i < jsonK.length())
                        {
                            Log.v("Query","inside MatrixCursor iterator" );
                            cursorQuery.addRow(new Object[]{jsonK.get(i), jsonV.get(i)});
                            Log.v("Query","inside MatrixCursor key: "+ jsonK.get(i).toString()+ "value: "+ jsonV.get(i).toString());
                            i++;
                        }
                        Log.v("Query", "Final query result received key: "+ DatabaseUtils.dumpCursorToString(cursorQuery));
                        cursor.moveToFirst();
                        while (!cursor.isAfterLast())
                        {
                            Object[] values = {cursor.getString(0), cursor.getString(1)};
                            cursorQuery.addRow(values);
                            cursor.moveToNext();
                        }
                        Log.v("Query", "Final query result received key: "+ DatabaseUtils.dumpCursorToString(cursorQuery));
                        return cursorQuery;
                    }
                }
                else
                {
                    Log.v("Query", "query for key");
                    try {
                        selhash = genHash(selection);
                        porthash = genHash(portStr);
                        predhash = genHash(pred);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                    if ((predhash.compareTo(selhash) < 0) && (selhash.compareTo(porthash) <= 0))
                    {
                        Log.v("Query|First If", "pred:" + predhash + "succ:" + succ);
                        cursor = myDB.query(sqlDB, selection);
                        Log.v("Query|First If", "Found key: "+selection+" at port:" + portStr);
                        return cursor;
                    }
                    else if ((predhash.compareTo(porthash) > 0) && (((selhash.compareTo(predhash) > 0) || (selhash.compareTo(porthash) < 0))))
                    {
                        Log.v("Query|Second If", "pred:" + predhash + "succ:" + succ);
                        cursor = myDB.query(sqlDB, selection);
                        Log.v("Query|Second If", "Found key: "+selection+" at port:" +  portStr);
                        return cursor;
                    }
                    else
                    {
                        Log.v("Query|Else", "pred:" + predhash + "succ:" + succ);
                        Message msg = new Message();
                        msg.setK(selection);
                        Log.v("Query","testing selectionArgs "+selectionArgs);
                        if(selectionArgs!=null && selectionArgs[0]!=null)
                        {
                            Log.v("Query","testing SelectionArgs!=null");
                            Log.v("Query", "first query port:");
                            String firstQueryPort = selectionArgs[0];
                            msg.setSourcePort(firstQueryPort);
                            Log.v("Query", "first query port:"+ firstQueryPort);
                        }
                        else
                        {
                            Log.v("Query","testing SelectionArgs==null");
                            Log.v("Query", "got from portStr:"+ portStr);
                            msg.setSourcePort(portStr);
                        }
                        msg.setTxt("Query");
                        Log.v("Query", "not found forwarding"+ msg.toString() + "to succ: "+ succ);
                        String receivedJsonSTring = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Integer.toString(Integer.parseInt(succ) * 2)).get();
                        Log.v("Query","Query result received from client as : "+receivedJsonSTring);
                        JSONObject json = new JSONObject(receivedJsonSTring);
                        JSONArray jsonK = json.getJSONArray("key");
                        JSONArray jsonV = json.getJSONArray("value");
                        Log.v("Query","Getting jsonKeyArray :"+ jsonK.toString() + "jsonValueArray :"+ jsonV.toString() );
                        MatrixCursor cursorQuery = new MatrixCursor(new String[]{"key", "value"});
                        int i = 0;
                        while (i < jsonK.length())
                        {
                            Log.v("Query","inside MatrixCursor iterator" );
                            cursorQuery.addRow(new Object[]{jsonK.get(i), jsonV.get(i)});
                            Log.v("Query","inside MatrixCursor key: "+ jsonK.get(i).toString()+ "value: "+ jsonV.get(i).toString());
                            i++;
                        }
                        Log.v("Query", "Final query result received key: "+ DatabaseUtils.dumpCursorToString(cursorQuery));
                        return cursorQuery;

                    }
                }
            }
        }
        catch(InterruptedException e){
            e.printStackTrace();
            Log.v("Query method","Interrupted Exception");
        } catch(ExecutionException e){
            e.printStackTrace();
            Log.v("Query method","Interrupted Exception");
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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
