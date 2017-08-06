/*
 * Title: CloudSim Toolkit Description: CloudSim (Cloud Simulation) Toolkit for Modeling and
 * Simulation of Clouds Licence: GPL - http://www.gnu.org/copyleft/gpl.html
 * 
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.cloudbus.cloudsim.core.CloudSim;


/**
 * Vm represents a VM: it runs inside a Host, sharing hostList with other VMs. It processes
 * cloudlets. This processing happens according to a policy, defined by the CloudletScheduler. Each
 * VM has a owner, which can submit cloudlets to the VM to be executed
 * 
 * @author Rodrigo N. Calheiros
 * @author Anton Beloglazov
 * @since CloudSim Toolkit 1.0
 */
public class Vm {

	/** The id. */
	private int id;

	/** The user id. */
	private int userId;

	/** The uid. */
	private String uid;

	/** The size. */
	private long size;

	/** The MIPS. */
	private double mips;

	/** The number of PEs. */
	private int numberOfPes;

	/** The ram. */
	private int ram;

	/** The bw. */
	private long bw;

	/** The vmm. */
	private String vmm;

	/** The Cloudlet scheduler. */
	private CloudletScheduler cloudletScheduler;

	/** The host. */
	private Host host;

	/** In migration flag. */
	private boolean inMigration;

	/** The current allocated size. */
	private long currentAllocatedSize;

	/** The current allocated ram. */
	private int currentAllocatedRam;

	/** The current allocated bw. */
	private long currentAllocatedBw;

	/** The current allocated mips. */
	private List<Double> currentAllocatedMips;

	/** The VM is being instantiated. */  
	private boolean beingInstantiated;

	/** The mips allocation history. */
	private final List<VmStateHistoryEntry> stateHistory = new LinkedList<VmStateHistoryEntry>();
	private static final int SLOT=300;
	private Map<Integer, Double> utilizationHistory=new TreeMap<Integer, Double>();
	private Map<Integer, Double> productUsage=new HashMap<Integer, Double>();
	public Map<Integer, Double> vm300=new HashMap<Integer, Double>();
	private static BigDecimal Fi1=new BigDecimal(-0.509);
	private static BigDecimal Fi2=new BigDecimal(-0.21);
	private static int previousTime=0;
	private static double previousUsage=0.0;
	/**
	 * Creates a new VMCharacteristics object.
	 * n 
	 * @param id unique ID of the VM
	 * @param userId ID of the VM's owner
	 * @param mips the mips
	 * @param numberOfPes amount of CPUs
	 * @param ram amount of ram
	 * @param bw amount of bandwidth
	 * @param size amount of storage
	 * @param vmm virtual machine monitor
	 * @param cloudletScheduler cloudletScheduler policy for cloudlets
	 * @pre id >= 0
	 * @pre userId >= 0
	 * @pre size > 0
	 * @pre ram > 0
	 * @pre bw > 0
	 * @pre cpus > 0
	 * @pre priority >= 0
	 * @pre cloudletScheduler != null
	 * @post $none
	 */
	public Vm(
			int id,
			int userId,
			double mips,
			int numberOfPes,
			int ram,
			long bw,
			long size,
			String vmm,
			CloudletScheduler cloudletScheduler) {
		setId(id);
		setUserId(userId);
		setUid(getUid(userId, id));
		setMips(mips);
		setNumberOfPes(numberOfPes);
		setRam(ram);
		setBw(bw);
		setSize(size);
		setVmm(vmm);
		setCloudletScheduler(cloudletScheduler);

		setInMigration(false);
		setBeingInstantiated(true);

		setCurrentAllocatedBw(0);
		setCurrentAllocatedMips(null);
		setCurrentAllocatedRam(0);
		setCurrentAllocatedSize(0);
		utilizationHistory.put(0,0.0);
		vm300.put(0,0.0);
		initProductUsage();
	}

	/**
	 * Updates the processing of cloudlets running on this VM.
	 * 
	 * @param currentTime current simulation time
	 * @param mipsShare array with MIPS share of each Pe available to the scheduler
	 * @return time predicted completion time of the earliest finishing cloudlet, or 0 if there is no
	 *         next events
	 * @pre currentTime >= 0
	 * @post $none
	 */
	public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
		if (mipsShare != null) {
			 double time=getCloudletScheduler().updateVmProcessing(currentTime, mipsShare);
			 addUtilization(currentTime);
			return time;
		}
		return 0.0;
	}

	/**
	 * Gets the current requested mips.
	 * 
	 * @return the current requested mips
	 */
	public List<Double> getCurrentRequestedMips() {
		List<Double> currentRequestedMips = getCloudletScheduler().getCurrentRequestedMips();
		if (isBeingInstantiated()) {
			currentRequestedMips = new ArrayList<Double>();
			for (int i = 0; i < getNumberOfPes(); i++) {
				currentRequestedMips.add(getMips());
			}
		}
		return currentRequestedMips;
	}

	/**
	 * Gets the current requested total mips.
	 * 
	 * @return the current requested total mips
	 */
	public double getCurrentRequestedTotalMips() {
		double totalRequestedMips = 0;
		for (double mips : getCurrentRequestedMips()) {
			totalRequestedMips += mips;
		}
		return totalRequestedMips;
	}

	/**
	 * Gets the current requested max mips among all virtual PEs.
	 * 
	 * @return the current requested max mips
	 */
	public double getCurrentRequestedMaxMips() {
		double maxMips = 0;
		for (double mips : getCurrentRequestedMips()) {
			if (mips > maxMips) {
				maxMips = mips;
			}
		}
		return maxMips;
	}

	/**
	 * Gets the current requested bw.
	 * 
	 * @return the current requested bw
	 */
	public long getCurrentRequestedBw() {
		if (isBeingInstantiated()) {
			return getBw();
		}
		return (long) (getCloudletScheduler().getCurrentRequestedUtilizationOfBw() * getBw());
	}

	/**
	 * Gets the current requested ram.
	 * 
	 * @return the current requested ram
	 */
	public int getCurrentRequestedRam() {
		if (isBeingInstantiated()) {
			return getRam();
		}
		return (int) (getCloudletScheduler().getCurrentRequestedUtilizationOfRam() * getRam());
	}
	public void setUid(String uid) {
		this.uid = uid;
	}

	/**
	 * Get unique string identificator of the VM.
	 * 
	 * @return string uid
	 */
	public String getUid() {
		return uid;
	}

	/**
	 * Generate unique string identificator of the VM.
	 * 
	 * @param userId the user id
	 * @param vmId the vm id
	 * @return string uid
	 */
	public static String getUid(int userId, int vmId) {
		return userId + "-" + vmId;
	}

	/**
	 * Gets the id.
	 * 
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * Sets the id.
	 * 
	 * @param id the new id
	 */
	protected void setId(int id) {
		this.id = id;
	}

	/**
	 * Sets the user id.
	 * 
	 * @param userId the new user id
	 */
	protected void setUserId(int userId) {
		this.userId = userId;
	}

	/**
	 * Gets the ID of the owner of the VM.
	 * 
	 * @return VM's owner ID
	 * @pre $none
	 * @post $none
	 */
	public int getUserId() {
		return userId;
	}

	/**
	 * Gets the mips.
	 * 
	 * @return the mips
	 */
	public double getMips() {
		return mips;
	}

	/**
	 * Sets the mips.
	 * 
	 * @param mips the new mips
	 */
	protected void setMips(double mips) {
		this.mips = mips;
	}

	/**
	 * Gets the number of pes.
	 * 
	 * @return the number of pes
	 */
	public int getNumberOfPes() {
		return numberOfPes;
	}

	/**
	 * Sets the number of pes.
	 * 
	 * @param numberOfPes the new number of pes
	 */
	protected void setNumberOfPes(int numberOfPes) {
		this.numberOfPes = numberOfPes;
	}

	/**
	 * Gets the amount of ram.
	 * 
	 * @return amount of ram
	 * @pre $none
	 * @post $none
	 */
	public int getRam() {
		return ram;
	}

	/**
	 * Sets the amount of ram.
	 * 
	 * @param ram new amount of ram
	 * @pre ram > 0
	 * @post $none
	 */
	public void setRam(int ram) {
		this.ram = ram;
	}

	/**
	 * Gets the amount of bandwidth.
	 * 
	 * @return amount of bandwidth
	 * @pre $none
	 * @post $none
	 */
	public long getBw() {
		return bw;
	}

	/**
	 * Sets the amount of bandwidth.
	 * 
	 * @param bw new amount of bandwidth
	 * @pre bw > 0
	 * @post $none
	 */
	public void setBw(long bw) {
		this.bw = bw;
	}

	/**
	 * Gets the amount of storage.
	 * 
	 * @return amount of storage
	 * @pre $none
	 * @post $none
	 */
	public long getSize() {
		return size;
	}

	/**
	 * Sets the amount of storage.
	 * 
	 * @param size new amount of storage
	 * @pre size > 0
	 * @post $none
	 */
	public void setSize(long size) {
		this.size = size;
	}

	/**
	 * Gets the VMM.
	 * 
	 * @return VMM
	 * @pre $none
	 * @post $none
	 */
	public String getVmm() {
		return vmm;
	}

	/**
	 * Sets the VMM.
	 * 
	 * @param vmm the new VMM
	 */
	protected void setVmm(String vmm) {
		this.vmm = vmm;
	}

	/**
	 * Sets the host that runs this VM.
	 * 
	 * @param host Host running the VM
	 * @pre host != $null
	 * @post $none
	 */
	public void setHost(Host host) {
		this.host = host;
	}

	/**
	 * Gets the host.
	 * 
	 * @return the host
	 */
	public Host getHost() {
		return host;
	}

	/**
	 * Gets the vm scheduler.
	 * 
	 * @return the vm scheduler
	 */
	public CloudletScheduler getCloudletScheduler() {
		return cloudletScheduler;
	}

	/**
	 * Sets the vm scheduler.
	 * 
	 * @param cloudletScheduler the new vm scheduler
	 */
	protected void setCloudletScheduler(CloudletScheduler cloudletScheduler) {
		this.cloudletScheduler = cloudletScheduler;
	}

	/**
	 * Checks if is in migration.
	 * 
	 * @return true, if is in migration
	 */
	public boolean isInMigration() {
		return inMigration;
	}

	/**
	 * Sets the in migration.
	 * 
	 * @param inMigration the new in migration
	 */
	public void setInMigration(boolean inMigration) {
		this.inMigration = inMigration;
	}

	/**
	 * Gets the current allocated size.
	 * 
	 * @return the current allocated size
	 */
	public long getCurrentAllocatedSize() {
		return currentAllocatedSize;
	}

	/**
	 * Sets the current allocated size.
	 * 
	 * @param currentAllocatedSize the new current allocated size
	 */
	protected void setCurrentAllocatedSize(long currentAllocatedSize) {
		this.currentAllocatedSize = currentAllocatedSize;
	}

	/**
	 * Gets the current allocated ram.
	 * 
	 * @return the current allocated ram
	 */
	public int getCurrentAllocatedRam() {
		return currentAllocatedRam;
	}

	/**
	 * Sets the current allocated ram.
	 * 
	 * @param currentAllocatedRam the new current allocated ram
	 */
	public void setCurrentAllocatedRam(int currentAllocatedRam) {
		this.currentAllocatedRam = currentAllocatedRam;
	}

	/**
	 * Gets the current allocated bw.
	 * 
	 * @return the current allocated bw
	 */
	public long getCurrentAllocatedBw() {
		return currentAllocatedBw;
	}

	/**
	 * Sets the current allocated bw.
	 * 
	 * @param currentAllocatedBw the new current allocated bw
	 */
	public void setCurrentAllocatedBw(long currentAllocatedBw) {
		this.currentAllocatedBw = currentAllocatedBw;
	}

	/**
	 * Gets the current allocated mips.
	 * 
	 * @return the current allocated mips
	 * @TODO replace returning the field by a call to getCloudletScheduler().getCurrentMipsShare()
	 */
	public List<Double> getCurrentAllocatedMips() {
		return currentAllocatedMips;
	}

	/**
	 * Sets the current allocated mips.
	 * 
	 * @param currentAllocatedMips the new current allocated mips
	 */
	public void setCurrentAllocatedMips(List<Double> currentAllocatedMips) {
		this.currentAllocatedMips = currentAllocatedMips;
	}

	/**
	 * Checks if is being instantiated.
	 * 
	 * @return true, if is being instantiated
	 */
	public boolean isBeingInstantiated() {
		return beingInstantiated;
	}

	/**
	 * Sets the being instantiated.
	 * 
	 * @param beingInstantiated the new being instantiated
	 */
	public void setBeingInstantiated(boolean beingInstantiated) {
		this.beingInstantiated = beingInstantiated;
	}

	/**
	 * Gets the state history.
	 * 
	 * @return the state history
	 */
	public List<VmStateHistoryEntry> getStateHistory() {
		return stateHistory;
	}

	/**
	 * Adds the state history entry.
	 * 
	 * @param time the time
	 * @param allocatedMips the allocated mips
	 * @param requestedMips the requested mips
	 * @param isInMigration the is in migration
	 */
	public void addStateHistoryEntry(
			double time,
			double allocatedMips,
			double requestedMips,
			boolean isInMigration) {
		VmStateHistoryEntry newState = new VmStateHistoryEntry(
				time,
				allocatedMips,
				requestedMips,
				isInMigration);
		if (!getStateHistory().isEmpty()) {
			VmStateHistoryEntry previousState = getStateHistory().get(getStateHistory().size() - 1);
			if (previousState.getTime() == time) {
				getStateHistory().set(getStateHistory().size() - 1, newState);
				return;
			}
		}
		getStateHistory().add(newState);
	}
	
	

	public List<? extends Cloudlet>  getExecList() {
		return getCloudletScheduler().getExecList();
	}

	public Map<Integer, Double> getUtilizationHistory() {
		return utilizationHistory;
	}

	public void setUtilizationHistory(Map<Integer, Double> utilizationHistory) {
		this.utilizationHistory = utilizationHistory;
	}

	/**
	 * Get utilization created by all clouddlets running on this VM.
	 * 
	 * @param time the time
	 * @return total utilization
	 */
	
	public int getAvaliablePEs(double time){
		int avaliablePe=getCloudletScheduler().getAvaliablePEs();
		
		int key=(int)(time/300);
		double maxusage=0;
		if(productUsage.containsKey(key))
			maxusage=productUsage.get(key);
		
		for (int i=1;i<SLOT/300;i++){
			if(productUsage.containsKey(key+i))
				if(maxusage<productUsage.get(key+i))
					maxusage=productUsage.get(key+i);
				}
		avaliablePe-=(int)Math.ceil(maxusage*getNumberOfPes());
		
		return avaliablePe;
	}
	
	private void initProductUsage(){
		try {
			Class.forName("com.mysql.jdbc.Driver");// 加载驱动程序
			Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/predict", "root","");// 连续数据库
			Statement statement = conn.createStatement();// statement用来执行SQL语句
			//String select_sql = "select * from product_usage";
			
			String select_sql="select * from product";
			ResultSet result=null;
			try {  
	            statement = conn.prepareStatement(select_sql);  
	            result = statement.executeQuery(select_sql);
	            
	            while(result.next()){
	            	int time=Integer.parseInt(result.getString("time"));
	            	double cpu=Double.valueOf(result.getString("cpuUsage"));
	            	productUsage.put(time,cpu);
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
	}

	private void addUtilization(double time){
		BigDecimal use=new BigDecimal(getNumberOfPes()-getAvaliablePEs(time));
		use=use.divide(new BigDecimal(getNumberOfPes()));//Log.printLine("before:"+use.doubleValue());
		//int key=(int)(time/300);
		//if(productUsage.containsKey(key))
			//use=use.add(new BigDecimal(productUsage.get(key)));
		//Log.printLine("end:"+use.doubleValue()+"key="+key);
		
		double utilization=use.doubleValue();
		int key=(int)(time/300);
		if(vm300.containsKey(key)) {
			if(vm300.get(key)<utilization){
				vm300.remove(key);
				vm300.put(key, utilization);
			}
		}
		else
			vm300.put(key, utilization);
			
		if(key>1){
			int old=key-1;
			while(!vm300.containsKey(old))
				old--;
			utilization=vm300.get(old);
			old++;
			while(old<key){
				vm300.put(old, utilization);
				old++;
			}
		}
		 key=(int)(time/SLOT);
		if(getUtilizationHistory().containsKey(key)) {
			if(getUtilizationHistory().get(key)<utilization){
				getUtilizationHistory().remove(key);
				getUtilizationHistory().put(key, utilization);
			}
		}
		else
			getUtilizationHistory().put(key, utilization);
			
		if(key>1){
			int old=key-1;
			while(!getUtilizationHistory().containsKey(old))
				old--;
			utilization=getUtilizationHistory().get(old);
			old++;
			while(old<key){
				getUtilizationHistory().put(old, utilization);
				old++;
			}
		}
	}
	
	
	private void updateFi(int key){
		/*int num=SLOT/300;
		int index=key/num;
		BigDecimal x[]=new BigDecimal[index];
		while(index>0){
			x[index-1]=new BigDecimal(getUtilizationHistory().get(key));
			key=key-num;
			index--;
		}*/
		BigDecimal x[]=new BigDecimal[getUtilizationHistory().size()];
		for(int i=0;i<x.length;i++)
			x[i]=new BigDecimal(getUtilizationHistory().get(i));
		BigDecimal n=new BigDecimal(x.length);
			BigDecimal u=new BigDecimal(0);
			for(int t=0;t<x.length;t++)
				u=u.add(x[t]);
			u=u.divide(n, 10, BigDecimal.ROUND_HALF_DOWN);
			BigDecimal theta=new BigDecimal(0);
			BigDecimal squar=new BigDecimal(0);
			for(int t=0;t<x.length;t++){
				squar=x[t].subtract(u);
				squar=squar.multiply(squar);
				theta=theta.add(squar);			
			}
			theta=theta.divide(n, 10, BigDecimal.ROUND_HALF_DOWN);
			BigDecimal r0=u.add(theta);
			
			BigDecimal squar1=new BigDecimal(0);
			BigDecimal r1=new BigDecimal(0);
			for(int t=0;t<x.length-1;t++){
				squar=x[t].subtract(u);
				squar1=x[t+1].subtract(u);
				squar=squar.multiply(squar1);
				r1=r1.add(squar);			
			}
			r1=r1.divide(n.subtract(new BigDecimal(1)), 10, BigDecimal.ROUND_HALF_DOWN);
			
			BigDecimal r2=new BigDecimal(0);
			for(int t=0;t<x.length-2;t++){
				squar=x[t].subtract(u);
				squar1=x[t+2].subtract(u);
				squar=squar.multiply(squar1);
				r2=r2.add(squar);			
			}
			r2=r2.divide(n.subtract(new BigDecimal(2)), 10, BigDecimal.ROUND_HALF_DOWN);
			
			BigDecimal up1=r1.multiply(r0);
			BigDecimal up2=r1.multiply(r2);
			BigDecimal down1=r0.multiply(r0);
			BigDecimal down2=r1.multiply(r1);
			up1=up1.subtract(up2);
			down1=down1.subtract(down2);
			if(down1.doubleValue()==0.0)
				Fi1=up1;
			else
				Fi1=up1.divide(down1, 10, BigDecimal.ROUND_HALF_DOWN);
			
			up1=r0.multiply(r2);
			up2=r1.multiply(r1);
			up1=up1.subtract(up2);
			down1=down1.subtract(down2);
			if(down1.doubleValue()==0.0)
				Fi2=up1;
			else
				Fi2=up1.divide(down1, 10, BigDecimal.ROUND_HALF_DOWN);
	}
	
	public int getAvaliablePEsPridictGratis(double time){
		int nowPE=getAvaliablePEs(time);
		int key=(int)(time/SLOT);
		//int num=SLOT/300;//
		//if (key<3*num)
		if(key<3)
			return nowPE;
		updateFi(key);
		BigDecimal a=new BigDecimal(getUtilizationHistory().get(key));
		//b=new BigDecimal(getUtilizationHistory().get(key-num));
		//c=new BigDecimal(getUtilizationHistory().get(key-2*num));
		BigDecimal b=new BigDecimal(getUtilizationHistory().get(key-1));
		BigDecimal c=new BigDecimal(getUtilizationHistory().get(key-2));
		
		
		BigDecimal predict=a.multiply(Fi1.add(new BigDecimal(1))).add(b.multiply(Fi2.subtract(Fi1))).subtract(c.multiply(Fi2));
		if(predict.compareTo(new BigDecimal(0))<0)
			predict=new BigDecimal(0);
		if(predict.compareTo(new BigDecimal(1))>0)
			predict=new BigDecimal(1);
			
		if(predict.doubleValue()>=1)
			return 0;
		BigDecimal pe=predict.multiply(new BigDecimal(getNumberOfPes()));
		int avaPE=getNumberOfPes()-(int)Math.ceil(pe.doubleValue());
		if(avaPE>nowPE)
			return nowPE;
		else
			return avaPE;
	}
	public int getAvaliablePEsPridictBatch(double time){
		int key=(int)(time/300);
		double max=0.0;
		if(productUsage.containsKey(key))
			max=productUsage.get(key);
		for(int i=0;i<SLOT/300;i++){
			if(productUsage.containsKey(key+i+1)){
				if(max<productUsage.get(key+i+1))
					max=productUsage.get(key+i+1);
			}
		}
					
		int avaPE=getCloudletScheduler().getAvaliablePEs()-(int)Math.ceil(max*getNumberOfPes());
		if(avaPE>0)
			return avaPE;
		else
			return 0;
	}
	public int getAvaliablePEsARMA(double time){
		int key=(int)(time/300);
		//int nowPE=getAvaliablePEs(time);
		//if (key<3)
		//	return nowPE;

		BigDecimal a=new BigDecimal(getUtilizationHistory().get(key));
		BigDecimal b=new BigDecimal(getUtilizationHistory().get(key-1));
		// batch=new BigDecimal(getUtilizationHistory().get(key));

		//BigDecimal max=batch;
		//xn=(ar-ma)*x(n-1)+ma*x(n-2)
				//ar=0.995 ma=0.318
		//batch=a.multiply(new BigDecimal(0.413)).add(b.multiply(new BigDecimal(0.586)));
		BigDecimal predict=a.multiply(new BigDecimal(0.667)).add(b.multiply(new BigDecimal(0.318)));
		
		if(predict.compareTo(new BigDecimal(0))<0)
			predict=new BigDecimal(0);
		if(predict.compareTo(new BigDecimal(1))>0)
			predict=new BigDecimal(1);
			
		if(predict.doubleValue()>=1)
			return 0;
		BigDecimal pe=predict.multiply(new BigDecimal(getNumberOfPes()));
		int avaPE=getNumberOfPes()-(int)Math.ceil(pe.doubleValue());
		//if(avaPE>nowPE)
			//return nowPE;
		//else
			return avaPE;
	}
	

}
