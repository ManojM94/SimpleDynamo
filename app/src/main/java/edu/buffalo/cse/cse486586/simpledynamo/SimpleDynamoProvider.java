package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import android.annotation.SuppressLint;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.net.Uri;
import android.os.AsyncTask;
import android.provider.UserDictionary;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.widget.TextView;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static android.content.ContentValues.TAG;
import static android.provider.UserDictionary.Words._ID;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoActivity.class.getSimpleName();
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final String[] AVD = {"5554","5556","5558","5560", "5562"};
	static final String[] AVD_PORTS = {"11108","11112","11116","11120", "11124"};
	static final int SERVER_PORT = 10000;
	NavigableMap<String, String> treeMap = new TreeMap<String, String>();
	NavigableMap<String, ArrayList<Integer>> partitionMap = new TreeMap<String, ArrayList<Integer>>();
	String selfHash;
	String prevNode = "null";
	String nextNode = "null";
	String prevNodeHash;
	String nextNodeHash;
	private Semaphore semaphore;
	static int c=0;
	private HashMap<String, ArrayList<String>> activities;
	private ContentResolver mContentResolver;

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");


	static final String PROVIDER_NAME = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	static final String URL = "content://" + PROVIDER_NAME ;
	static final Uri CONTENT_URI = Uri.parse(URL);
	String myPort;
	/**
	 * Database specific constant declarations
	 */

	private SQLiteDatabase db;
	DatabaseHelper dbHelper;
	static final String DATABASE_NAME = "MSG";
	static final String MESSAGES_TABLE_NAME = "messages";
	static final int DATABASE_VERSION = 1;
	static final String CREATE_DB_TABLE =
			" CREATE TABLE " + MESSAGES_TABLE_NAME +
					"([key] TEXT PRIMARY KEY, " +
					"value TEXT NOT NULL);";

	/**
	 * Helper class that actually creates and manages
	 * the provider's underlying data repository.
	 */


	@Override
	public boolean onCreate() {
		Context context = getContext();
		mContentResolver = getContext().getContentResolver();
		semaphore = new Semaphore(1, true);
		activities = new HashMap<String, ArrayList<String>>();
		dbHelper = new DatabaseHelper(context);
		db = dbHelper.getWritableDatabase();
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		try {
			selfHash = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "No such algo on create");
		}

		for(int i=0;i<5;i++) {

			try {
				String hash = genHash(AVD[i]);
				treeMap.put(hash, AVD_PORTS[i]);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
			Map.Entry h_entry = treeMap.higherEntry(selfHash);
			Map.Entry l_entry = treeMap.lowerEntry(selfHash);
		if(h_entry!=null){
			nextNode = h_entry.getValue().toString();
			nextNodeHash = h_entry.getKey().toString();
		}
		else {
			nextNode = treeMap.firstEntry().getValue();
			nextNodeHash = treeMap.firstEntry().getKey();
		}
		if(l_entry!=null){
			prevNode = l_entry.getValue().toString();
			prevNodeHash = l_entry.getKey().toString();
		}
		else{
			prevNode = treeMap.lastEntry().getValue();
			prevNodeHash = treeMap.lastEntry().getKey();
		}
		Log.e(TAG, "Previous node "+prevNode);
		Log.e(TAG, "My node "+myPort);
		Log.e(TAG, "Next node "+nextNode);
		ArrayList<Integer> part0 = new ArrayList<Integer>();
		part0.add(4);
		part0.add(1);
		ArrayList<Integer> part1 = new ArrayList<Integer>();
		part0.add(0);
		part0.add(2);
		ArrayList<Integer> part2 = new ArrayList<Integer>();
		part0.add(1);
		part0.add(3);
		ArrayList<Integer> part3 = new ArrayList<Integer>();
		part0.add(2);
		part0.add(4);
		ArrayList<Integer> part4 = new ArrayList<Integer>();
		part0.add(3);
		part0.add(0);
		partitionMap.put(AVD_PORTS[0], part0);
		partitionMap.put(AVD_PORTS[1], part1);
		partitionMap.put(AVD_PORTS[2], part2);
		partitionMap.put(AVD_PORTS[3], part3);
		partitionMap.put(AVD_PORTS[4], part4);
		try{
			semaphore.acquire();
			new RecoveryClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");

		}
		return db!=null;
	}


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		SQLiteDatabase sqLiteDatabase = dbHelper.getWritableDatabase();

		String valHash = "";
		try {
			valHash = genHash(selection);
			if(selection.contains(":") && !selection.contains("*")){
				valHash = genHash(selection.split(":")[0]);
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		int response = 0;
		if (selection.equals("@")) {
			response = sqLiteDatabase.delete(MESSAGES_TABLE_NAME, null, null);
			Log.e(TAG, "Response to delete1 "+ Integer.toString(response));
		} else if (selection.equals("*")) {
			response = sqLiteDatabase.delete(MESSAGES_TABLE_NAME, null, null);
			Log.e(TAG, "Response to delete2 "+ Integer.toString(response));

			try {
				String resp = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Delete:"+myPort+":"+selection, nextNode).get();
				if(resp.equals("Failed")){
					Map.Entry nextofNextEntry = treeMap.higherEntry(nextNodeHash);
					String nextofNext;
					if(nextofNextEntry!=null){
						nextofNext = nextofNextEntry.getValue().toString();
					}
					else{
						nextofNext = treeMap.firstEntry().getValue();
					}
					resp =  new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "Delete:"+myPort+":"+selection, nextofNext).get();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}


		}  else {
			String owner = getInsertOwner(valHash);

			try {
				Log.e(TAG, "I was asked to delete "+ selection+" from owner" + owner);
				String resp = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Delete:"+myPort+":"+selection, owner).get();
				if (resp.equals("Failed")) {
					Map.Entry nextofOwnerEntry = treeMap.higherEntry(genHash(owner));
					String nextofOwner;
					if(nextofOwnerEntry!=null){
						nextofOwner = nextofOwnerEntry.getValue().toString();
					}
					else{
						nextofOwner = treeMap.firstEntry().getValue();
					}
					String resp1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Delet1:"+owner+":"+selection, nextofOwner).get();
					if(resp1.equals("Failed")){
						resp1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Delete:"+myPort+":"+selection, owner).get();
					}
					return 1;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		Log.e(TAG, "Returning response "+ Integer.toString(response));
		return 1;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		String valHash = "";

		try {
			valHash = genHash(values.getAsString("key"));
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "No such algo for value");
		}
		String owner = getInsertOwner(valHash);


			Log.e(TAG, "Forwarding to " + owner+" "+ values.toString());
			JSONObject jsonObject = new JSONObject();
			try {
				jsonObject.put("key", values.getAsString("key"));
				jsonObject.put("value", values.getAsString("value"));
			} catch (JSONException e) {
				e.printStackTrace();
			}
		try {
			String resp = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Insert-" +myPort+"-" +jsonObject.toString(), owner).get();
			Log.e(TAG, "Response after forwarding to " + owner+" "+ resp);
			if(resp.equals("Successful")){
				return uri;
			}else if(resp.equals("Failed")){
				Map.Entry nextofOwnerEntry = treeMap.higherEntry(genHash(Integer.toString(Integer.parseInt(owner)/2)));
				String nextofOwner;
				if(nextofOwnerEntry!=null){
					nextofOwner = nextofOwnerEntry.getValue().toString();
				}
				else{
					nextofOwner = treeMap.firstEntry().getValue();
				}
				Log.e(TAG,"Sending to next of owner "+"Inser1-" +owner+"-"+ jsonObject.toString()+":"+nextofOwner);
				String resp1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Inser1-" +owner+"-"+ jsonObject.toString(), nextofOwner).get();
				Log.e(TAG, "Response from next of owner "+resp1);
				if(resp1.equals("Successful")){
					return uri;
				}else if(resp1.equals("Failed")){
					Log.e(TAG,"Retrying owner "+"Inser1-" +owner+"-"+ jsonObject.toString());
					resp = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Insert-"+myPort+"-" + jsonObject.toString(), owner).get();
					return uri;
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Log.e(TAG, "Insert forwarded to " + owner);
		return null;

	}



	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public MatrixCursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
							  String sortOrder) {
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		qb.setTables(MESSAGES_TABLE_NAME);
		String[] p = {"key", "value"};

		String valHash = "";
		try {
			valHash = genHash(selection);
			if(selection.contains(":") && !selection.contains("*")){
				valHash = genHash(selection.split(":")[0]);
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		if (selection.equals("@")) {
			selection = null;

			Cursor c = qb.query(db,	p,	selection,
					selectionArgs,null, null, sortOrder);

			MatrixCursor matrixCursor = new MatrixCursor(p);
			c.moveToFirst();
			while (!c.isAfterLast()) {
				Object[] val = {c.getString(0), c.getString(1)};
				matrixCursor.addRow(val);
				c.moveToNext();
			}
			c.close();
			Log.e(TAG, "Returning values "+c.toString());
			return matrixCursor;
		} else if (selection.equals("*")) {
			MatrixCursor matrixCursor = new MatrixCursor(p);
			try {
				Log.e(TAG, "I was asked * by tester");
				String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query:"+myPort+":"+selection, nextNode).get();
				if (response.equals("Failed")) {
					Map.Entry nextofNextEntry = treeMap.higherEntry(nextNodeHash);
					String nextofNext;
					if(nextofNextEntry!=null){
						nextofNext = nextofNextEntry.getValue().toString();
					}
					else{
						nextofNext = treeMap.firstEntry().getValue();
					}
					response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query:"+myPort+":"+selection, nextofNext).get();
					Log.e(TAG, "This is the response1 " + response);

				}
				if (!response.equals("I got nothing")) {
					String t = response.substring(response.length()-200);
					Log.e(TAG,"Here it is"+t);
					String[] res = response.split(",");

					for (int i = 0; i < res.length; i++) {
						if(res[i].split(":").length>1)
							matrixCursor.addRow(res[i].split(":"));
					}
				}
				selection = null;
				Cursor c = qb.query(db,	p,	selection,
						selectionArgs,null, null, sortOrder);
				c.moveToFirst();
				while (!c.isAfterLast()) {
					Object[] val = {c.getString(0), c.getString(1)};
					matrixCursor.addRow(val);
					c.moveToNext();
				}
				c.close();
				Log.e(TAG, Integer.toString(matrixCursor.getCount()));
				return matrixCursor;
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

		}  else {
			String owner = getQueryOwner(valHash);
			Log.e(TAG, "I am " + myPort + " I was asked by tester " + selection + " "+ owner);
			try {
				String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query:"+myPort+":"+selection, owner).get();
				if(response.equals("Failed")){
					Map.Entry prevofOwnerEntry = treeMap.lowerEntry(genHash(Integer.toString(Integer.parseInt(owner)/2)));
					String prevofOwner;
					if(prevofOwnerEntry!=null){
						prevofOwner = prevofOwnerEntry.getValue().toString();
					}
					else{
						prevofOwner = treeMap.lastEntry().getValue();
					}
					response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query:"+myPort+":"+selection, prevofOwner).get();
					if(response.equals("Failed")){
						response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query:"+myPort+":"+selection, owner).get();
						Log.e(TAG, "Query failed at "+owner);
					}else{
						Log.e(TAG, "Query failed at next of owner");
					}
				}else{
					Log.e(TAG, "Query failed at owner");
				}

				Log.e(TAG, "This is the response " + response);
				String[] res = response.split(",");


				MatrixCursor matrixCursor = new MatrixCursor(p);
				for(int i=0; i<res.length; i++){
					if(res[i].split(":").length>1)
						matrixCursor.addRow(res[i].split(":"));
				}
				return matrixCursor;
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

		}
		return null;

	}




	private synchronized String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private synchronized String insertToSelf(String i){
		try{
			semaphore.acquire();
			Log.e(TAG, "Insert to self "+i);
			String[] query = i.split("-");
			String msg = query[2].replace("{", "").replace("}", "").replace("\"","");
			String[] msgs = msg.split(",");
			String value = msgs[0].substring(6);
			String key = msgs[1].substring(4);
			ContentValues mContentValues = new ContentValues();
			mContentValues.put(KEY_FIELD, key);
			mContentValues.put(VALUE_FIELD, value);
			db.insertWithOnConflict(MESSAGES_TABLE_NAME, null, mContentValues, db.CONFLICT_REPLACE);
			String asker = query[1];
			if(i.contains("Insert")){
				asker = myPort;
			}
			if(activities.containsKey(asker)){
				activities.get(asker).add("Insert,"+key+","+value);
			}else{
				ArrayList<String> temp = new ArrayList<String>();
				temp.add("Insert,"+key+","+value);
				activities.put(asker, temp);
			}
			if(!i.contains("Inser2")){
				if(i.contains("Insert")){
					query[1] = myPort;
					query[0] = "Inser1";
				}else{
					query[0]="Inser2";
				}

			i = query[0]+"-"+query[1]+"-"+query[2];
			String resp = sendMessage(i, nextNode, 10000);
			Log.e(TAG,"Response from nextnode "+nextNode+" insertion "+resp);
			if(resp.equals("Failed")) {
				if (i.contains("Inser1")) {
					query[0] = "Inser2";
					i = query[0] + "-" + query[1] + "-" + query[2];
					Map.Entry nextofNextEntry = treeMap.higherEntry(nextNodeHash);
					String nextofNext;
					if (nextofNextEntry != null) {
						nextofNext = nextofNextEntry.getValue().toString();
					} else {
						nextofNext = treeMap.firstEntry().getValue();
					}
					String resp1 = sendMessage(i, nextofNext, 10000);
					Log.e(TAG,"Response from nextofnextnode "+nextofNext+" insertion "+resp1);
					if (resp1.equals("Failed")) {
						query[0] = "Inser1";
						i = query[0] + "-" + query[1] + "-" + query[2];
						resp = sendMessage(i, nextNode, 10000);
						semaphore.release();
						return resp;
					}
					semaphore.release();
					return resp1;
				}
			}
			semaphore.release();
			return resp;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		semaphore.release();
		return "Successful";
	}

private synchronized String queryToSelf(String i) {
	try {
		semaphore.acquire();
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		qb.setTables(MESSAGES_TABLE_NAME);
		String[] query = i.split(":");
		String reqType = query[0];
		String msgPort = query[1];
		String selection = query[2];
		String[] p ={"key","value"};
		if (!selection.equals("*")) {
			selection = "\"" + selection.split(":")[0] + "\"";
			qb.appendWhere("[key]" + "=" + selection);
			selection = null;

			Cursor c = qb.query(db, p, selection,
					null, null, null, null);
			c.moveToFirst();
			String resp = "I have nothing";
			if (c.getCount() > 0) {
				resp = c.getString(0)+":"+c.getString(1);
				Log.e(TAG, "Returning result from self "+ resp);
			} else {
				query[1] = myPort;
				i = query[0] + ":" + query[1] + ":" + query[2];
				String res = sendMessage(i, prevNode, 1500);
				if (res.equals("Failed")) {
					Map.Entry prevofPrevEntry = treeMap.lowerEntry(prevNodeHash);
					String prevofPrev;
					if (prevofPrevEntry != null) {
						prevofPrev = prevofPrevEntry.getValue().toString();
					} else {
						prevofPrev = treeMap.lastEntry().getValue();
					}
					res = sendMessage(i, prevofPrev,1500);
				}
				Log.e(TAG, "Returning result from query to self "+ res);
				semaphore.release();
				return res;
			}
			c.close();
			Log.e(TAG, "Returning result from query to self1 "+ resp);
			semaphore.release();
			return resp;
		} else {
			String resp = null;
			if (!msgPort.equals(nextNode)) {
				resp = sendMessage(i, nextNode, 10000);
				Log.e(TAG, "I was asked * and so I asked next "+msgPort +resp);
				if (resp.equals("Failed")) {
					Map.Entry nextofNextEntry = treeMap.higherEntry(nextNodeHash);
					String nextofNext;
					if (nextofNextEntry != null) {
						nextofNext = nextofNextEntry.getValue().toString();
					} else {
						nextofNext = treeMap.firstEntry().getValue();
					}
					if (!msgPort.equals(nextofNext)) {
						resp = sendMessage(i, nextofNext, 10000);
					}
				}
			}
			String result = ",";
			Cursor cursor = db.rawQuery("Select [key], value from " + MESSAGES_TABLE_NAME, null);
			cursor.moveToFirst();
			while (!cursor.isAfterLast()) {
				String val = cursor.getString(0) + ":" + cursor.getString(1);
				result = result + val;
				cursor.moveToNext();
				result = result + ",";
			}

			semaphore.release();
			if(resp!=null)
				return result.substring(1, result.length()-1)+","+resp;
			return result.substring(1, result.length()-1);
		}

	} catch (InterruptedException e) {
		e.printStackTrace();
	}
	return "";
}

	private synchronized int deleteInSelf(String i) {
		try {
			semaphore.acquire();
			SQLiteDatabase sqLiteDatabase = dbHelper.getWritableDatabase();
			String[] query = i.split(":");
			String reqType = query[0];
			String msgPort = query[1];
			String selection = query[2];
			Map.Entry nextofNextEntry = treeMap.higherEntry(nextNodeHash);
			String nextofNext;
			if (nextofNextEntry != null) {
				nextofNext = nextofNextEntry.getValue().toString();
			} else {
				nextofNext = treeMap.firstEntry().getValue();
			}
			if (!selection.equals("*")) {
				String[] whereArgs = {selection};
				sqLiteDatabase.delete(MESSAGES_TABLE_NAME, "[key]=?", whereArgs);
				Log.e(TAG, "I have deleted "+selection+" from my table");
				if(reqType.equals("Delete")){
					msgPort = myPort;
				}
				if(activities.containsKey(msgPort)){
					activities.get(msgPort).add("Delete,"+selection);
				}else{
					ArrayList<String> temp =  new ArrayList<String>();
					temp.add("Delete,"+selection);
					activities.put(msgPort, temp);
				}
				if(!reqType.equals("Delet2")){
					if(reqType.equals("Delete")){
						query[1] = myPort;
						query[0] = "Delet1";
					}else{
						query[0] = "Delet2";
					}
					i = query[0]+":"+query[1]+":"+query[2];
					reqType = query[0];
					msgPort = query[1];
					Log.e(TAG,"I'm Insert owner asking to delete "+i);
					String resp =  sendMessage(i, nextNode, 10000);
					if(resp.equals("Failed")){
						if(reqType.equals("Delet1")){
							query[0]="Delet2";
							reqType = query[0];
							i = query[0]+":"+query[1]+":"+query[2];

							String resp1 = sendMessage(i, nextofNext, 10000);
							if(resp1.equals("Failed")){
								query[0]="Delet1";
								i = query[0]+":"+query[1]+":"+query[2];
								resp1 = sendMessage(i, nextNode, 10000);
							}
						}
					}
				}
			}else{
				if(!msgPort.equals(nextNode)){
					String resp = sendMessage(i, nextNode, 10000);
					if(resp.equals("Failed")){
						if(!msgPort.equals(nextNode)){
							String resp1 = sendMessage(i, nextofNext, 10000);
						}
					}
				}
				sqLiteDatabase.delete(MESSAGES_TABLE_NAME, null, null);

			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		Log.e(TAG,"After deletion, I am returning");
		semaphore.release();
		return 0;
	}

		private synchronized String sendMessage(String i, String port, int timeout){
		try{
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port));
			if(timeout!=1500)
				timeout=5000;
			socket.setSoTimeout(timeout);
			PrintWriter printWriter= new PrintWriter(socket.getOutputStream());
			Log.e(TAG, "Sending message "+i+" to "+port);
			printWriter.println(i);
			printWriter.flush();
			String resp = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
			Log.e(TAG, "Receiving message "+resp +" from  "+ port);
			if(resp==null){
				resp = "Failed";
			}
			socket.close();
			return resp;
		}catch (SocketTimeoutException e){
			e.printStackTrace();
		}
		catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "Failed";
	}








	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			try {
				while (!Thread.interrupted()) {
					Socket s = serverSocket.accept();
					String i = new BufferedReader(new InputStreamReader(s.getInputStream())).readLine();
					Log.e(TAG, "Received from client "+ i);
					PrintWriter printWriter= new PrintWriter(s.getOutputStream());

					if(i.contains("Insert")|| i.contains("Inser1")||i.contains("Inser2")){
						String response = insertToSelf(i);
						printWriter.println(response);
						printWriter.flush();
						Log.e(TAG, "Insert to self -"+i+"-"+ response);
						s.close();

					}
					else if(i.contains("Query")){
						String response = queryToSelf(i);
						printWriter.println(response);
						printWriter.flush();
						Log.e(TAG, "Query to self "+response);
						s.close();
					}
					else if(i.contains("Delete")||i.contains("Delet1")||i.contains("Delet2")){
						String response = Integer.toString(deleteInSelf(i));
						Log.e(TAG, "Delete in self "+response);
						printWriter.println(response);
						printWriter.flush();
						s.close();
					}else if(i.contains("Recovery")){
						String[] query = i.split(":");
						String acts = "";
						for(int j=1; j<4; j++){
							if(activities.containsKey(query[j])){
								acts = acts+ activities.get(query[j]).toString();
							}
						}
						printWriter.println(acts);
						printWriter.flush();
						Log.e(TAG, "These are the acts "+query[1]+":"+acts);
						s.close();
					}

				}
			}
			catch (IOException e)
			{
				Log.e(TAG, "Can't accept connection");
			}

			return null;
		}

		protected void onProgressUpdate(String...strings) {
			/*
			 * The following code displays what is received in doInBackground().
			 */

			return;
		}
	}


	private class RecoveryClientTask extends AsyncTask<Void, Void, Void>{
		@Override
		protected Void doInBackground(Void... voids){
			SharedPreferences sharedPreferences = getContext().getSharedPreferences("DynamoPref", Context.MODE_PRIVATE);
			if(!sharedPreferences.contains("Initial")){
				SharedPreferences .Editor editor = sharedPreferences.edit();
				editor.putInt("Initial", 0);
				editor.commit();
				Log.e("RecoveryClientTask", "Set shared pref");
				semaphore.release();
				return null;
			}
			Log.e("RecoveryClientTask", "Fetching values from others");
			Map.Entry prevofPrevEntry = treeMap.lowerEntry(prevNodeHash);
			String prevofPrev;
			if (prevofPrevEntry != null) {
				prevofPrev = prevofPrevEntry.getValue().toString();
			} else {
				prevofPrev = treeMap.lastEntry().getValue();
			}
			String msg = "Recovery"+":"+myPort+":"+ prevNode+ ":"+prevofPrev;
			Log.e(TAG, "Sending recovery message "+msg+":"+nextNode);

				String prevResp = sendMessage(msg, prevNode, 10000);

				String nextResp = sendMessage(msg, nextNode, 10000);

				Log.e(TAG, "Resending recovery message to prev "+prevResp);
				if(prevResp.equals("Failed")){
					prevResp = sendMessage(msg, prevNode, 10000);
				}
				Log.e(TAG, "Resending recovery message to next "+nextResp);
				if(nextResp.equals("Failed")){

					nextResp = sendMessage(msg, nextNode, 10000);

				}


				Log.e(TAG,"These are the prev Activities before "+prevResp);
				Log.e(TAG,"These are the next Activities before "+nextResp);
				prevResp = prevResp.replaceAll("\\[", "");
				prevResp = prevResp.replaceAll("\\]", ", ");

				nextResp = nextResp.replaceAll("\\[", "");
				nextResp = nextResp.replaceAll("\\]", ", ");
				Log.e(TAG,"These are the prev Activities "+prevResp);
				Log.e(TAG,"These are the next Activities "+nextResp);

				if(prevResp.length()!=0){
					String[] preAct = prevResp.split(", ");
					for (int i =0; i<preAct.length;i++) {
						String[] act = preAct[i].split(",");
						if (act[0].equals("Insert")) {
							ContentValues values = new ContentValues();
							values.put("key", act[1]);
							values.put("value", act[2]);
							db.insertWithOnConflict(MESSAGES_TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
						} else if (act[0].equals("Delete")) {
							String[] whereArgs = {act[1]};
							db.delete(MESSAGES_TABLE_NAME, "[key]=?", whereArgs);
						}
					}
				}
				if(nextResp.length()!=0){
					String[] nextAct = nextResp.split(", ");
					for (int i =0; i<nextAct.length;i++) {
						String[] act = nextAct[i].split(",");
						if (act[0].equals("Insert")) {
							ContentValues values = new ContentValues();
							values.put("key", act[1]);
							values.put("value", act[2]);
							db.insertWithOnConflict(MESSAGES_TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
						} else if (act[0].equals("Delete")) {
							String[] whereArgs = {act[1]};
							db.delete(MESSAGES_TABLE_NAME, "[key]=?", whereArgs);
						}
					}
				}

			semaphore.release();
			return null;
		}

	}



	/***
	 * ClientTask is an AsyncTask that should send a string over the network.
	 * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
	 * an enter key press event.
	 *
	 * @author stevko
	 *
	 */
	private class ClientTask extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... msgs) {
			try {

					//System.out.println("Connecting to successor");
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(msgs[1]));
					socket.setSoTimeout(5000);
					//Log.e(TAG, "Sending to server "+ msgs[0] + " port "+ msgs[1]);
					PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
					printWriter.println(msgs[0]);
					printWriter.flush();
					String i = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
					//Log.e(TAG, "Received from server "+ i + " port " + msgs[1]);
					if(i==null){
						i = "Failed";
					}
					socket.close();
					return i;



			}catch (SocketTimeoutException e){
				Log.e(TAG, "SocketTimeout at Client");
			}

			catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");

			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");

			}
			return "Failed";
		}
	}












	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}


	private static class DatabaseHelper extends SQLiteOpenHelper {
		DatabaseHelper(Context context){
			super(context, DATABASE_NAME, null, DATABASE_VERSION);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL(CREATE_DB_TABLE);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			db.execSQL("DROP TABLE IF EXISTS " +  MESSAGES_TABLE_NAME);
			onCreate(db);
		}

	}
	private synchronized String getInsertOwner(String valhash){
		Iterator iterator = treeMap.entrySet().iterator();
		while(iterator.hasNext()) {
			Map.Entry entry = (Map.Entry) iterator.next();
			String curhash = entry.getKey().toString();
			String nexthash = treeMap.higherKey(curhash);
			if (nexthash != null) {
				if (valhash.compareTo(curhash) > 0 && valhash.compareTo(nexthash) <= 0){
					return treeMap.get(nexthash);
				}
			}

		}
		return treeMap.firstEntry().getValue();
	}

	private synchronized String getQueryOwner(String valhash){
		Iterator iterator = treeMap.entrySet().iterator();
		while(iterator.hasNext()) {
			Map.Entry entry = (Map.Entry) iterator.next();
			String curhash = entry.getKey().toString();
			String nexthash = treeMap.higherKey(curhash);
			if (nexthash != null) {
				if (valhash.compareTo(curhash) > 0 && valhash.compareTo(nexthash) <= 0){
					String nextKey = treeMap.higherKey(nexthash);
					if(nextKey==null){
						nextKey = treeMap.firstKey();
					}
					nextKey = treeMap.higherKey(nextKey);
					if(nextKey==null){
						nextKey = treeMap.firstKey();
					}
					return treeMap.get(nextKey);
				}
			}

		}
		return treeMap.higherEntry(treeMap.higherKey(treeMap.firstKey())).getValue();
	}
}
