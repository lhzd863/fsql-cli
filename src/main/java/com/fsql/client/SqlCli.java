package com.fsql.client;



import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.fsql.client.cli.CliOptions;
import com.fsql.client.cli.CliOptionsParser;
import com.fsql.client.cli.SqlCommandParser;
import com.fsql.client.cli.SqlCommandParser.SqlCommandCall;
import com.fsql.client.util.CliStrings;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class SqlCli {

    public static void main(String[] args) throws Exception {
        final CliOptions options = CliOptionsParser.parseClient(args);
        SqlCli submit = new SqlCli(options);
        submit.run();
    }

    private String sqlf;
    private String jobName = "test";
    private TableEnvironment tEnv;
    StreamTableEnvironment stEnv;
    StreamExecutionEnvironment sEnv;
    StreamTableEnvironment tableEnv ;

    private SqlCli(CliOptions options) {
        this.sqlf = options.getSqlf();
        this.jobName = options.getJobName();
    }

    private void run() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.enableCheckpointing(60000L,CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
		
        tableEnv = StreamTableEnvironment.create(env,settings);
        
//        List<String> sql = Files.readAllLines(Paths.get(sqlf));
        ArrayList<String> list=new ArrayList<String>();
        List<String> sql = java.util.Arrays.asList(sqlf.split("[\n|\r\n]"));
        List<SqlCommandCall> calls = SqlCommandParser.parse(sql);
        boolean ret = true;
        for (SqlCommandCall call : calls) {
        	switch (call.command) {
			  case ERRQUIT:
				  if (!ret) System.exit(1);
				  break;
			  default:
        	      ret = callCommand(call);
        	}
        }
    }

    // --------------------------------------------------------------------------------------------

    private boolean callCommand(SqlCommandCall cmdCall) {
    	boolean ret = true;
		switch (cmdCall.command) {
			case RESET:
				callReset();
				break;
			case SET:
				callSet(cmdCall);
				break;
			case HELP:
				callHelp();
				break;
			case SHOW_CATALOGS:
				callShowCatalogs(cmdCall);
				break;
			case SHOW_DATABASES:
				callShowDatabases();
				break;
			case SHOW_TABLES:
				callShowTables();
				break;
			case SHOW_FUNCTIONS:
				callShowFunctions();
				break;
			case SHOW_MODULES:
				callShowModules();
				break;
			case USE_CATALOG:
				callUseCatalog(cmdCall);
				break;
			case USE:
				callUseDatabase(cmdCall);
				break;
			case DESC:
			case DESCRIBE:
				callDescribe(cmdCall);
				break;
			case EXPLAIN:
				callExplain(cmdCall);
				break;
			case SELECT:
				callSelect(cmdCall);
				break;
			case INSERT_INTO:
			case INSERT_OVERWRITE:
				ret = callInsert(cmdCall);
				break;
			case CREATE_TABLE:
				ret = callDdl(cmdCall.operands[0], CliStrings.MESSAGE_TABLE_CREATED);
				break;
			case DROP_TABLE:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_TABLE_REMOVED);
				break;
			case CREATE_VIEW:
				ret = callDdl(cmdCall.operands[0], CliStrings.MESSAGE_VIEW_CREATED);
				break;
			case DROP_VIEW:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_VIEW_REMOVED);
				break;
			case CREATE_FUNCTION:
				ret = callDdl(cmdCall.operands[0], CliStrings.MESSAGE_FUNCTION_CREATED);
				break;
			case DROP_FUNCTION:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_FUNCTION_REMOVED);
				break;
			case ALTER_FUNCTION:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_ALTER_FUNCTION_SUCCEEDED,
						CliStrings.MESSAGE_ALTER_FUNCTION_FAILED);
				break;
			case SOURCE:
				callSource(cmdCall);
				break;
			case CREATE_DATABASE:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_DATABASE_CREATED);
				break;
			case DROP_DATABASE:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_DATABASE_REMOVED);
				break;
			case ALTER_DATABASE:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_ALTER_DATABASE_SUCCEEDED,
						CliStrings.MESSAGE_ALTER_DATABASE_FAILED);
				break;
			case ALTER_TABLE:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_ALTER_TABLE_SUCCEEDED,
						CliStrings.MESSAGE_ALTER_TABLE_FAILED);
				break;
			case CREATE_CATALOG:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_CATALOG_CREATED);
				break;
			case DROP_CATALOG:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_CATALOG_REMOVED);
				break;
			default:
				throw new RuntimeException("Unsupported command: " + cmdCall.command);
		}
		return ret;
	}

    private void callReset() {
		printInfo(CliStrings.MESSAGE_RESET);
	}
    
    private void callShowDatabases() {
    	printInfo(CliStrings.MESSAGE_EMPTY);
	}
    
    private void callHelp() {
		printInfo(CliStrings.MESSAGE_EMPTY);
	}
    
    private boolean callShowCatalogs(SqlCommandCall cmdCall) {
    	printInfo(CliStrings.MESSAGE_EMPTY);
    	return true;
	}
    
    private void printInfo(String message) {
		System.out.println(message);
	}
    private void callShowTables() {
    	printInfo(CliStrings.MESSAGE_EMPTY);
	}
    private void callShowFunctions() {
    	printInfo(CliStrings.MESSAGE_EMPTY);
	}
    private void callShowModules() {
    	printInfo(CliStrings.MESSAGE_EMPTY);
	}
    private void callUseCatalog(SqlCommandCall cmdCall) {
    	
    	printInfo(CliStrings.MESSAGE_EMPTY);
	}
    private void callUseDatabase(SqlCommandCall cmdCall) {
    	printInfo(CliStrings.MESSAGE_EMPTY);
	}
    private void callDescribe(SqlCommandCall cmdCall) {
    	printInfo(CliStrings.MESSAGE_EMPTY);
	}
    private void callExplain(SqlCommandCall cmdCall) {
    	printInfo(CliStrings.MESSAGE_EMPTY);
	}
    private void callSelect(SqlCommandCall cmdCall) {
    	printInfo(CliStrings.MESSAGE_EMPTY);
	}
    private void callSource(SqlCommandCall cmdCall) {
    	printInfo(CliStrings.MESSAGE_EMPTY);
	}
    private boolean callDdl(String ddl, String successMessage) {
    	return callDdl(ddl, successMessage, null);
	}
    private boolean callDdl(String ddl, String successMessage, String errorMessage) {
    	boolean ret = true;
    	System.out.println(ddl);
        try {
        	((TableEnvironment)tableEnv).executeSql(ddl);
        } catch (SqlParserException e) {
        	ret = false;
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
        return ret;
    }
    
    private boolean callSet(SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        
        tableEnv.getConfig().getConfiguration().setString(key, value);
        return true;
    }
    
    private boolean callInsert(SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        System.out.println(dml);
        boolean ret = true;
        try {
        	((TableEnvironment)tableEnv).executeSql(dml);
        } catch (SqlParserException e) {
        	ret = false;
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
        return ret;
    }
    
}
