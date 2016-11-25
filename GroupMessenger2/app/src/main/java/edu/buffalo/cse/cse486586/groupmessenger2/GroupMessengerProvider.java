package edu.buffalo.cse.cse486586.groupmessenger2;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.util.Log;


public class GroupMessengerProvider extends ContentProvider {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();

    static final Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider");


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
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
        long recordNumber=-1;
        try {
            recordNumber = myDB.insert(GroupMessengerDatabase.TABLE_NAME, null, values);
        } catch(SQLiteException e) {
            Log.v("cannot", e.toString());
        }
        Log.v("insert", values.toString());
        return CONTENT_URI.buildUpon().appendEncodedPath(String.valueOf(recordNumber)).build();
    }

    @Override
    public boolean onCreate() {
        newDatabase = new GroupMessengerDatabase(getContext());
        return false;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        final SQLiteDatabase myDB = newDatabase.getReadableDatabase();
        SQLiteQueryBuilder newQueryBuilder = new SQLiteQueryBuilder();
        newQueryBuilder.setTables(GroupMessengerDatabase.TABLE_NAME);
        Cursor queryCursor = myDB.rawQuery("SELECT key, value FROM gmtable WHERE key = ?", new String[]{selection});
        Log.v("query", selection);
        return queryCursor;
    }
}
