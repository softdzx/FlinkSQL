package pingle.wang.sqlserver.flink.job;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * @Author: wpl
 */
public class JobCompilationResult {
    private  JobGraph jobGraph;
    private  List<Path> additionalJars;

    public JobCompilationResult() {
    }

    JobCompilationResult(JobGraph jobGraph, List<Path> additionalJars) {
        this.jobGraph = jobGraph;
        this.additionalJars = additionalJars;
    }

    public JobGraph jobGraph() {
        return jobGraph;
    }

    public List<Path> additionalJars() {
        return additionalJars;
    }
}
