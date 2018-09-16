package edu.buffalo.cse.cse486586.simpledht;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.util.Log;


/**
 * Created by tejaswini on 4/5/18.
 */

public class MyDBHandler extends SQLiteOpenHelper {

    public MyDBHandler(Context context, String name, SQLiteDatabase.CursorFactory factory, int version) {
        super(context, name, factory, version);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        sqLiteDatabase.execSQL("CREATE TABLE GroupMessenger('key' TEXT PRIMARY KEY, 'value' TEXT)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {
    }

    public void insert(SQLiteDatabase db, ContentValues val) {
        db.insertWithOnConflict("GroupMessenger", null, val, SQLiteDatabase.CONFLICT_REPLACE);
        //Log.v("GroupMessengerProvider","insert-> query :"+ DatabaseUtils.dumpCursorToString(cursor));
        //Cursor cursor = db.query("GroupMessenger", null, null, null, null, null, null);
    }

    public Cursor query(SQLiteDatabase db, String key) {
        Cursor cursor = db.query("GroupMessenger", null, "key=?", new String[]{key}, null, null, null, null);
        Log.v("query", key);
        return cursor;
    }

    public Cursor queryAll(SQLiteDatabase db, String key) {
        Cursor cursor = db.query("GroupMessenger", null, null, null, null, null, null, null);
        Log.v("query", key);
        return cursor;
    }

    public void delete(SQLiteDatabase db, String key) {
        db.delete("GroupMessenger", "key=?", new String[]{key});
    }

    public void deleteAll(SQLiteDatabase db) {
        db.delete("GroupMessenger", null, null);
    }
}
