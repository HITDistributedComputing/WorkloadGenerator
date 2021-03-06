package cn.hit.cst.ssl.fbgen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class WorkloadGenerator {
	private enum Framework {
		MR, Spark, Tez
	}
	//private MRGenerator mrGenerator;
	private ArrayList<JobGenerator> jobGenerators;
	private static String traceFilePath = "/home/hadoop/workspace/workloads/trace/FB-2009_0_700.tsv";
	private Random random;
	/*
    *
    * Parses a tab separated file into an ArrayList<ArrayList<String>>
    *
    */
	
	//should be initialized while generating workload rather
	//than fixed to be one kind of framework while constructing
	public WorkloadGenerator(){
		this.jobGenerators = new ArrayList<JobGenerator>();
		//this.jobGenerators.add(new MRGenerator());
		this.jobGenerators.add(new SparkGenerator());
		this.random = new Random(System.currentTimeMillis());
	}
	
   public static void parseFileArrayList(String path, 
					  ArrayList<ArrayList<String>> data 
					  ) throws Exception {
	
	/*long maxInput = 0;*/

	BufferedReader input = new BufferedReader(new FileReader(path));
	String s;
	String[] array;
	int rowIndex = 0;
	int columnIndex = 0;
	while (true) {
	    if (!input.ready()) break;
	    s = input.readLine();
	    array = s.split("\t");
	    try {
		columnIndex = 0;
		while (columnIndex < array.length) {
		    if (columnIndex == 0) {
			data.add(rowIndex,new ArrayList<String>());
		    }
		    String value = array[columnIndex];
		    data.get(rowIndex).add(value);

		    /*if (Long.parseLong(array[INPUT_DATA_SIZE]) > maxInput) {
			maxInput = Long.parseLong(array[INPUT_DATA_SIZE]);
		    }*/

		    columnIndex++;
		}
		rowIndex++;
	    } catch (Exception e) {
		
	    }
	}
	input.close();
	
   }
   
   public static void main(String args[]){
	   WorkloadGenerator workloadGenerator = new WorkloadGenerator();
	   //workloadGenerator.generateWorkload();
	   try {
		workloadGenerator.generateMultiWorkloads(2, "/home/hadoop/workspace/workloads/shell/");
		//generate only 1 script for submitting applications
		//workloadGenerator.generateWorkload("/home/hadoop/workspace/workloads/shell/");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
   
   //single script workload will be generated in $dir/run_workload.sh
   public void generateWorkload(String dir){
	   ArrayList<ArrayList<String>> trace = new ArrayList<ArrayList<String>>();
	   JobGenerator jobGenerator;
	   try {
		   //Trace FileInput
		   parseFileArrayList(this.traceFilePath,
				   trace);
	   } catch (Exception e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
	   }
	   String[] jobCmds = new String[trace.size()];
	   //jobid submissionTime subInterval inputSize
	   int count = 0;
	   for (ArrayList<String> arrayList : trace) {
		   jobGenerator = randomJobGenerator();
		   jobCmds[count] = "sleep " + arrayList.get(2);
		   //TODO: random job frameworks, use specified framework generator
		   jobCmds[count] += "\n" + jobGenerator.jobGenerator(count, this.random);
		   count ++;
	   }
	   try {
		//RunWorkload FileOutput
		writeToShell(jobCmds, dir + "run_workload.sh");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	   return;
   }
   
   //parallel workload scripts will be generated in $dir/run_workload_0...shellCount
   public void generateMultiWorkloads(int shellCount, String dir) throws IOException{
	   ArrayList<ArrayList<String>> trace = new ArrayList<ArrayList<String>>();
	   JobGenerator jobGenerator;
	   try {
		   //Trace FileInput
		   parseFileArrayList(this.traceFilePath,
				   trace);
	   } catch (Exception e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
	   }
	   int jobCount, itrTimes, restJobCount, sleepTime, currentJob; 
	   int lastSubTime[] = new int[shellCount];
	   int startJobId = 1076;
	   jobCount = trace.size();
	   itrTimes = jobCount / shellCount;
	   restJobCount = jobCount % shellCount;
	   sleepTime = 0;
	   String[][] shells = new String[shellCount][itrTimes + 1];
	   for(int i = 0; i < itrTimes; i++){
		   for(int j = 0; j < shellCount; j++){
			   jobGenerator = randomJobGenerator();
			   currentJob = i * shellCount + j;
			   sleepTime = Integer.valueOf(trace.get(currentJob).get(1)) - lastSubTime[j];
			   shells[j][i] = "sleep " + sleepTime
					   + "\n" + jobGenerator.jobGenerator(currentJob + startJobId,
							   this.random);
			   lastSubTime[j] = Integer.valueOf(trace.get(currentJob).get(1));
		   }
	   }
	   for(int i = 0; i < restJobCount; i++){
		   jobGenerator = randomJobGenerator();
		   currentJob = jobCount - restJobCount + i;
		   sleepTime = Integer.valueOf(trace.get(currentJob).get(1)) - lastSubTime[i];
		   shells[i][itrTimes] = "sleep " + sleepTime
				   + "\n" + jobGenerator.jobGenerator(currentJob + startJobId,
						   this.random);
	   }
	   for (int i = 0; i < shellCount; i++) {
		   //RunWorkload FileOutput
		   writeToShell(shells[i],
				   dir + "run_workload_" + i + ".sh");
	   }
   }
   
   //params
   //@jobCmds:String[]
   //an element in String array jobCmds represent a command for a job
   //@fileName:String
   //the file path of target benchmark shell script
   public void writeToShell(String[] jobCmds, String fileName) throws IOException{
	   File runFile = new File(fileName);
	   if (!runFile.exists()) {
		   runFile.createNewFile();
	   }
	   else{
		   runFile.delete();
		   runFile.createNewFile();
	   }
	   BufferedWriter out = new BufferedWriter(new FileWriter(runFile));
	   String prepDirCmd = "rm -r benchLogs\nmkdir benchLogs\n";
	   out.write(prepDirCmd);
	   for(int i = 0; i < jobCmds.length - 1; i++){
		   out.write(jobCmds[i]);
	   }
	   if (jobCmds[jobCmds.length - 1] != null) {
		out.write(jobCmds[jobCmds.length - 1]);
	}
	   out.close();
	   return;
   }
   
//   public void writeToMultiShell(String[] jobCmds, int shellCount){
//	   int itrTimes = jobCmds.length / shellCount;
//	   int restJob = jobCmds.length % shellCount;
//	   String[][] shells = new String[shellCount][itrTimes + 1]; 
//	   //iteration
//	   for(int i = 0; i < itrTimes; i++){
//		   for(int j = 0; j < shellCount; j++){
//			   shells[j][itrTimes] = ""; 
//		   }
//	   }
//	   //TODO: 5805%4=the rest 1
//	   
//	   //TODO: out to file
//	   String[] fileNames = new String[shellCount];
//	   BufferedWriter[] out = new BufferedWriter[shellCount];
//	   for(int i = 0; i < shellCount; i++){
//		   fileNames[i] = "/home/hadoop/workspace/workloads/shell/parallel_workloads_" + i + ".sh";
//		   out[i] = new BufferedWriter(new FileWriter(new File(fileNames[i])));
//	   }
//	   for(int i = 0; i < jobCmds.length; i++){
//		   out[i].close();
//	   }
//	   return;
//   }
   
   //Actually, this is a random for choosing job framework: MR, Spark etc
   public JobGenerator randomJobGenerator(){
	   int frameworkCounter = this.jobGenerators.size();
	   int targetFramework = this.random.nextInt(frameworkCounter);
	   return this.jobGenerators.get(targetFramework);
   }
}
