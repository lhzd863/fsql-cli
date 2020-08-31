package com.fsql.client.cli;

public class CliOptions {

    private final String sqlf;
    private final String jobName;

    public CliOptions(String sqlf, String jobName) {
        this.sqlf = sqlf;
        this.jobName = jobName;
    }

    public String getSqlf() {
        return sqlf;
    }

	public String getJobName() {
		return jobName;
	}
}
