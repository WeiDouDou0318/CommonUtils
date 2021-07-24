package com.ddwei.folder.CreateFolder;

import java.io.*;

/**
 * CreateFolderAndFileTest
 *
 * @author 魏豆豆
 * @date 2021/6/25
 */
public class CreateFolderAndFileTest {
    //分别定义字符串目录文件路径，源文件前缀路径，生成文件前缀路径,和目录文件编码。使用时记得初始化
    private final String contentPath,sourcePre,targetPre,charset;

    public CreateFolderAndFileTest(){
        contentPath = "C:/Users/weijingli/Desktop/aaa/aaa.txt";
        sourcePre = "F:/kewai";
        targetPre = "C:/Users/weijingli/Desktop";
        charset = "GBK";
    }

    /**
     * 读取文件处理
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        CreateFolderAndFileTest test = new CreateFolderAndFileTest();
        File contentFile = new File(test.contentPath);
        if(!contentFile.getParentFile().exists()){
        	System.out.println("目录文件不存在！");
            return;
        }else{
            FileInputStream contentStream = new FileInputStream(contentFile);
            InputStreamReader isr = new InputStreamReader(contentStream, test.charset);
            BufferedReader br = new BufferedReader(isr);
            String lineText;
            while((lineText=br.readLine())!=null){
                test.lineHandle(lineText);
            }
        }


    }

    /**
     * 每行处理，将字符串处理成文件过程
     * @param commonPath
     */
    public void lineHandle(String commonPath){

        File sourceFile,targetFile;
        sourceFile = new File(sourcePre+commonPath);
        targetFile = new File(targetPre+commonPath);
        if(!targetFile.getParentFile().exists()){
            targetFile.getParentFile().mkdirs();
        }
        copyFile(sourceFile,targetFile);
    }

    /**
     * copy文件处理
     */
    public void copyFile(File sourceFile,File targetFile){

        try {
            FileInputStream in = new FileInputStream(sourceFile);
            FileOutputStream out = new FileOutputStream(targetFile);

            byte[] bs = new byte[1024];
            int count = 0;

            while(-1!=(count=in.read(bs,0,bs.length))){
                out.write(bs,0,count);
            }
            in.close();
            out.flush();
            out.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}