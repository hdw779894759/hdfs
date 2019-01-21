package com.imooc.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;

import java.util.Collection;

/**
 * 使用springboot的方式访问hdfs
 */
@SpringBootApplication
public class SpringBootHDFSApp implements CommandLineRunner {

    @Autowired
    FsShell fsShell;

    public void run(String... strings) throws Exception {
        Collection<FileStatus> fileStatuses = fsShell.lsr("/user/root");
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(">" + fileStatus.getPath());

        }
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBootHDFSApp.class, args);
    }
}
