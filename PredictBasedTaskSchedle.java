/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation
 *               of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009, The University of Melbourne, Australia
 */

package simu;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerPridict;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;


/**
 * A simple example showing how to create
 * two datacenters with one host each and
 * run two cloudlets on them.
 */
public class PredictBasedTaskSchedle {
	/*cpu&mem*/
	private static int peNormal=100000;
	private static int memNormal=1000000;
	private static int diskNormal=10000000;
	/** The cloudlet list. */
	private static List<Cloudlet> cloudletList;
	
	/** The vmlist. */
	private static List<Vm> vmlist;

	/**
	 * Creates main() to run this example
	 */
	public static void main(String[] args) {
		Log.printLine("Starting predictSchedule");

		try {
			// First step: Initialize the CloudSim package. It should be called
			// before creating any entities.
			int num_user = 1;   // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false;  // mean trace events

			// Initialize the GridSim library
			CloudSim.init(num_user, calendar, trace_flag);

			// Second step: Create Datacenters
			//Datacenters are the resource providers in CloudSim. We need at list one of them to run a CloudSim simulation
			Datacenter datacenter = createDatacenter("Datacenter");
			//Datacenter datacenter1 = createDatacenter("Datacenter_1");

			//Third step: Create Broker
			DatacenterBroker broker = createBroker();
			int brokerId = broker.getId();

			//Fourth step: Create one virtual machine
			vmlist = new ArrayList<Vm>();
			
			long size = 10000; //image size (MB)
			long bw = 1000;
			//int pesNumber = 1; //number of cpus
			String vmm = "Xen"; //VMM name
			
			//create 6 VMs,add the VMs to the vmList
			
			for(int vmId=0;vmId<10;vmId++){
				Vm vm;
				vm = new Vm(vmId, brokerId, 2000, peNormal, memNormal, bw, size, vmm, new CloudletSchedulerPridict());
				vmlist.add(vm);
			}	
			//submit vm list to the broker
			
			
			broker.submitVmList(vmlist);
			
			//Fifth step: Create two Cloudlets
			cloudletList = new ArrayList<Cloudlet>();
			//Cloudlet properties
			long fileSize = 100;
			long outputSize = 100;
			UtilizationModel utilizationModel = new UtilizationModelFull();
			Cloudlet cloudlet;
			try {
				Class.forName("com.mysql.jdbc.Driver");// 加载驱动程序
				Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/predict", "root","");// 连续数据库
				Statement statement = conn.createStatement();// statement用来执行SQL语句
				//String select_sql = "select * from workflow";
				//一天的数据量
				//String select_sql="select * from input<87400"; // 
				String select_sql="select * from input where priority<2";//>80000 and time<174800"; // 
				 
				ResultSet result=null;
				int cloudletID=0;
				try {  
		            statement = conn.prepareStatement(select_sql);  
		            result = statement.executeQuery(select_sql);
		            
		            while(result.next()){
		            	
		            	//int cloudletID=Integer.parseInt(result.getString("taskNum"));
		            	double cpu=Double.valueOf(result.getString("cpuReq"));
		            	int cpuReq=(int)(cpu*peNormal );
		            	double disk=Double.valueOf(result.getString("diskReq"));
		            	String jobid=result.getString("jobID");		            	
		            	
		            	//int length=(int)(result.getDouble("setTime")*2000*300*10);
		            	int length=(int)(cpuReq*100);
		            	//int length=(int)(disk*diskNormal*500);
		            	cloudlet = new Cloudlet(cloudletID, length, cpuReq, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
		            	//Log.printLine(cloudletID);
		            	
		             	cloudlet.setUserId(brokerId);
		            	cloudlet.setSubmitTime(Double.valueOf(result.getString("time")));
		            	cloudlet.setClassType((int)(result.getShort("priority")));
		            	cloudlet.setJobId(jobid);
		            	cloudletList.add(cloudlet);
		            	cloudletID++;
		            	}
				} catch (SQLException e) {  
		            e.printStackTrace();  
		        }				 
		        conn.close();   
			} catch(ClassNotFoundException e) {   
			System.out.println("Sorry,can`t find the Driver!");   
			e.printStackTrace();   
			} catch(SQLException e) {   
			e.printStackTrace();   
			} catch(Exception e) {   
			e.printStackTrace();   
			}   
			//submit cloudlet list to the broker
			
			broker.submitCloudletList(cloudletList);
			
			//bind the cloudlets to the vms. This way, the broker
			// will submit the bound cloudlets only to the specific VM
			//broker.bindCloudletToVm(cloudlet1.getCloudletId(),vm1.getId());
			//broker.bindCloudletToVm(cloudlet2.getCloudletId(),vm2.getId());

			// Sixth step: Starts the simulation
			CloudSim.startSimulation();


			// Final step: Print results when simulation is over
		
			CloudSim.stopSimulation();
			saveTaskevent(cloudletList);
			saveVMUtilization(vmlist);
				
        	Log.printLine("Predict schecule finished!");
		}
		catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}

	private static Datacenter createDatacenter(String name){

		// Here are the steps needed to create a Datacenter:
		// 1. We need to create a list to store
		//    our machine
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores.
		// In this example, it will have only one core.
		List<Pe> peList = new ArrayList<Pe>();
		//int mips = 1000;

		// 3. Create PEs and add these into a list.
		int peId=0;
		while(peId<peNormal)
		{
				peList.add(new Pe(peId, new PeProvisionerSimple(2000))); // need to store Pe id and MIPS Rating
				peId++;
		}
		
		//4. Create Host with its id and list of PEs and add them to the list of machines
		
		long storage = 1000000; //host storage
		int bw = 10000;
		
		//in this example, the VMAllocatonPolicy in use is SpaceShared. It means that only one VM
		//is allowed to run on each Pe. As each Host has only one Pe, only one VM can run on each Host.
		for(int hostId=0;hostId<10;hostId++)
			hostList.add(new Host(
						hostId,
						new RamProvisionerSimple(memNormal),
						new BwProvisionerSimple(bw),
						storage,
						peList,
						new VmSchedulerSpaceShared(peList))); // This is our first machine
		
				
		// 5. Create a DatacenterCharacteristics object that stores the
		//    properties of a data center: architecture, OS, list of
		//    Machines, allocation policy: time- or space-shared, time zone
		//    and its price (G$/Pe time unit).
		String arch = "x86";      // system architecture
		String os = "Linux";          // operating system
		String vmm = "Xen";
		double time_zone = 10.0;         // time zone this resource located
		double cost = 3.0;              // the cost of using processing in this resource
		double costPerMem = 0.05;		// the cost of using memory in this resource
		double costPerStorage = 0.001;	// the cost of using storage in this resource
		double costPerBw = 0.0;			// the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now

	       DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
	                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


		// 6. Finally, we need to create a PowerDatacenter object.
		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	//We strongly encourage users to develop their own broker policies, to submit vms and cloudlets according
	//to the specific rules of the simulated scenario
	private static DatacenterBroker createBroker(){

		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBroker("Broker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}
	
	private static void saveVMUtilization(List<Vm> vmlist){
		Map<Integer, Double> total=new HashMap<Integer, Double>();
		Map<Integer, Double> product=new HashMap<Integer, Double>();
		
		try
		{
			File csvRec = new File("/Users/oupeng/Documents/workspace/data/vmHistory-a.csv");
			BufferedWriter bw = new BufferedWriter(new FileWriter(csvRec, true)); 
		
			for(Vm vm:vmlist){
				//total=vm.getUtilizationHistory();
				total=vm.vm300;
				for (int key:total.keySet()){
					bw.newLine();
					bw.write(key+","+vm.getId()+","+total.get(key));
				}
        }
			bw.close(); 			
		}catch (FileNotFoundException e) {
			// 捕获File对象生成时的异常             
			e.printStackTrace();
			} catch (IOException e) {
				// 捕获BufferedWriter对象关闭时的异常
				e.printStackTrace();
				}
	}
	private static void saveTaskevent(List<Cloudlet> clist){
	//	Map<Double, String> event=new HashMap<Double, String>();
	
		try
		{
			File csvRec = new File("/Users/oupeng/Documents/workspace/data/taskEvent-a.csv");
			BufferedWriter bw = new BufferedWriter(new FileWriter(csvRec, true)); 
		
			for(Cloudlet cl:clist){
				/*event=cl.getEventHistory();
				for (Double key:event.keySet()){
					bw.newLine();
					bw.write(key+","+event.get(key));*/
				for(String record:cl.getEventHistory() ){
					bw.newLine();
					bw.write(record);
				}
        }
			bw.close(); 			
		}catch (FileNotFoundException e) {
			// 捕获File对象生成时的异常             
			e.printStackTrace();
			} catch (IOException e) {
				// 捕获BufferedWriter对象关闭时的异常
				e.printStackTrace();
				}
	}
}

