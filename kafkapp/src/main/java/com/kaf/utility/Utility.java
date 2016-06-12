package com.kaf.utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URL;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 * @author Rahul Vishwakarma
 *
 */
public class Utility {

    /**
     * Project utilities
     */
    private static final Logger log = LoggerFactory.getLogger(Utility.class);
    
    /**
     * Default constructor
     */
    public Utility() {
    }

    /**
     * Precision formatter on the double value
     * @param value
     * @return
     */
    public static double getTwoDecimalValue(double value){
    	return Double.parseDouble(new DecimalFormat("##.##").format(value));
    }
    
    public static double getDoubleFromString(String value){
    	Double v = 0d;
    	try{
    		if(isValidString(value)){
    			v = Double.valueOf(value);
    		}    		
    	} catch (NumberFormatException e) {
			log.debug(e.getMessage());
		}
    	return v;
    }

    public static int getIntegerFromString(String value){
    	Integer v = 0;
    	try{
    		if(isValidString(value)){
    			v = Integer.valueOf(value);
    		}    		
    	} catch (NumberFormatException e) {
			log.debug(e.getMessage());
		}
    	return v;
    }
    
    @Deprecated
    public static long getTimeFromTimestampString(String timestamp){
		long time = 0;
		boolean done = false;
		if(timestamp == null){
			return 0;
		}
		if(timestamp.isEmpty()){
			return 0;
		}
		if(!done){
			try{
				//Format 4/1/2013 12:00:00 AM
				SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
				time = dateFormat.parse(timestamp).getTime();
				done = true;
			} catch (ParseException e) {
				//log.debug("1. Unable to parse timestamp: "+e.getMessage());
			}
		}
		if(!done){
			try{
				//Format Aug 23 2013 12:38PM
				SimpleDateFormat dateFormat = new SimpleDateFormat("MMM dd yyyy HH:mma");
				time = dateFormat.parse(timestamp).getTime();
				done = true;
			} catch (ParseException e) {
				//log.debug("2. Unable to parse timestamp: "+e.getMessage());
			}
		}
		if(!done){
			try{
				//Format 2013-08-23T10:27:43.513
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
				time = dateFormat.parse(timestamp).getTime();
				done = true;
			} catch (ParseException e) {
				//log.debug("3. Unable to parse timestamp: "+e.getMessage());
			}
		}
		if(!done){
			try{
				//Format 2013-08-23T10:27:43.513
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				time = dateFormat.parse(timestamp).getTime();
				done = true;
			} catch (ParseException e) {
				//log.debug("4. Unable to parse timestamp: "+e.getMessage());
			}
		}
		if(!done){
			try{
				//Format  4/1/2013 00:00:00
				SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
				time = dateFormat.parse(timestamp).getTime();
				done = true;
			} catch (ParseException e) {
				//log.debug("6. Unable to parse timestamp: "+e.getMessage());
			}
		}
		if(!done){
			try{
				//Format 27/02/2001
				SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
				time = dateFormat.parse(timestamp).getTime();
				done = true;
			} catch (ParseException e) {
				//log.debug("5. Unable to parse timestamp: "+e.getMessage());
			}
		}
		if(!done){
			try{
				//Format  2008-09-12 00:00:00
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				time = dateFormat.parse(timestamp).getTime();
				done = true;
			} catch (ParseException e) {
				//log.debug("6. Unable to parse timestamp: "+e.getMessage());
			}
		}
		if(!done){
			try{
				//Format 40379.0141319444
				//This is assumed to be the time from Apple COCOA API
				//Logic is to add 978307200L to the long value				
				Date date= null;
				Double ddate = 0D ;
				ddate = Double.parseDouble(timestamp);
				long jValue = 0;			
				// Earlier than 1985 in Unix, must be Apple
				if(ddate < 473410800) {
					jValue =(long) ((ddate + 978307200L) * 1000);
				    date = new Date(jValue);
				} else {
					jValue =(long) (ddate * 1000);
				    date = new Date(jValue);
				}
				time = date.getTime();
				done = true;
			} catch (NumberFormatException e) {
				//log.debug("7. Unable to parse timestamp: "+e.getMessage());
			}
		}
		if(!done){
			log.debug("8. Unable to parse timestamp: "+timestamp);
		}
		return time;
	}
    
    /**
     * Parse: 7/27/09 1:16 PM
     * 
     * @param date
     * @return
     */
    public static Timestamp getTimestampFromStringForODT(String date){
    	DateFormat df = new SimpleDateFormat("MM/dd/yy HH:mm a", Locale.ENGLISH);
    	Timestamp timestamp = null;
    	try {
    		if(date== null || date.isEmpty())
    			return null;
    		timestamp =  new Timestamp(df.parse(date).getTime());
		} catch (ParseException e) {
			log.error(e.getMessage(),e);
		}
    	return timestamp;
    }
 
	/**
	 * Get the current time in UTC
	 * 
	 * @return
	 */
	public static Timestamp getCurrentUTCDateTime(){
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		return timestamp;
	}
 
    /** Check the String is null or not
     * 
     * @param value
     * @return
     */
    public static boolean isValidString(String value){
	if(value != null && value.trim().length()> 0)
	    return true;
	else
	    return false;
    }
    /**
     * Check whether string is valid url or not
     * 
     * @param url
     * @return
     */
    public static boolean isValidURL_old(String url){
	    InputStream inputStream = null;
	    try {
	         inputStream = new URL(url).openStream();
	    }catch (IOException e) {
	    	e.printStackTrace();
	    }
	    return (inputStream == null)?false:true;
	}
    
    /**
     * Check whether string is valid url or not
     * 
     * @param url
     * @return
     */
    public static boolean isValidURL(String url){	    
	    String urlRegex = "\\b(https?|ftp|file|ldap)://"
	            + "[-A-Za-z0-9+&@#/%?=~_|!:,.;]"
	            + "*[-A-Za-z0-9+&@#/%=~_|]";	    
	    	return url.matches(urlRegex);	    
	}

    /**
     * Generate Random double number
     * @param min
     * @param max
     * @return
     */
    public static double getRandomDoubleBetween(double min, double max) {
	Random random = new Random();
	double range = max - min;
	double scaled = random.nextDouble() * range;
	double shifted = Double.parseDouble(new DecimalFormat("##.##").format(scaled + min));
	return shifted;
    }

    /**
     * Format utility to convert strings to a title case
     * @param lDataSet
     * @return
     */
    public static List<String> formatListToTitleCase(List<String> lDataSet) throws Exception{
	String SPACE_DELIMITER = " ";
	List<String> formattedList = new ArrayList<String>();
	StringTokenizer st = null;
	StringBuffer strBuf = null;
	if(lDataSet != null && !lDataSet.isEmpty()){	
	    for(String strData : lDataSet){
		st = new StringTokenizer(strData,SPACE_DELIMITER);
		strBuf = new StringBuffer();
		while(st.hasMoreElements()){
		    String strToken = st.nextToken();
		    int index=0;
		    //Run through the characters and identify first alphabet to capitalize
		    char[] characterSet = strToken.toCharArray();
		    for(index=0;index<strToken.length();index++){
			if(isLatinLetter(characterSet[index])){
			    strBuf.append(strToken.substring(index,index+1).toUpperCase());
			    index++;
			    break;
			}else{
			    strBuf.append(strToken.substring(index,index+1));
			}
		    }
		    strBuf.append(strToken.substring(index).toLowerCase());
		    strBuf.append(SPACE_DELIMITER);
		}
		formattedList.add(strBuf.toString());
	    }
	}
	return formattedList;
    }
    
    /**
     * Valid letter for Latin
     * @param c character to check
     * @return boolean
     */
    public static boolean isLatinLetter(char c) {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
    }

    /**
     * Format String
     * @param str string to format
     * @return string formated string
     */
    public static String formatStringToTitleCase(String str){

	String SPACE_DELIMITER = " ";
	StringBuffer strBuf = new StringBuffer();

	if(str == null || str.trim().length() < 1 || str.equals("") || str.equals("null"))
	    return "";
	StringTokenizer st = new StringTokenizer(str,SPACE_DELIMITER);
	while(st.hasMoreElements()){
	    String strToken = st.nextToken();
	    char[] characterSet = strToken.toCharArray();
	    int index=0;
	    for(index=0;index<strToken.length();index++){
		if(isLatinLetter(characterSet[index])){
		    strBuf.append(strToken.substring(index,index+1).toUpperCase());
		    index++;
		    break;
		}else{
		    strBuf.append(strToken.substring(index,index+1));
		}
	    }
	    strBuf.append(strToken.substring(index).toLowerCase());
	    strBuf.append(SPACE_DELIMITER);
	}
	if(strBuf.length()>0)
	    strBuf.deleteCharAt((strBuf.length()-1));
	return strBuf.toString();
    }

    /**
     * Club the Key and Value
     * Separate them by space and contain value in small bracket
     * @param hMap
     * @return List<String>
     * @throws Exception 
     */
    public static List<String> formatCombineKeyValueFromMap(LinkedHashMap<String,String> hMap) throws Exception{
	List<String> lValues = new ArrayList<String>();
	for (Map.Entry<String,String> e : hMap.entrySet()) {
	    String formattedVal = e.getKey()+" ("+formatStringToTitleCase(e.getValue())+")";
	    lValues.add(formattedVal);
	}
	return lValues;
    }

    /**  Get the key by passing the value to map otherwise return null
     * 
     * @param map
     * @param value
     * @return
     */
    public static <T, E> T getKeyByValue(Map<T, E> map, E value) {
		for (Entry<T, E> entry : map.entrySet()) {
		    if (value.equals(entry.getValue())) {
			return entry.getKey();
		    }
		}
		return null;
    }

    /***
     *  Check array is empty or not
     * @param values
     * @return
     */
    public static boolean isValidIntegerArray(Integer[] values){
	if(values != null && values.length > 0 && values[0] > 0)
	    return true;
	return false;
    }

    /***
     *  Check array is empty or not
     * @param values
     * @return
     */
    public static boolean isValidStringArray(String[] values){
	if(values != null && values.length > 0 && !values[0].isEmpty())
	    return true;
	return false;
    }

    /**
     * Create a delimited string from the integer array
     * @param integers
     * @param delimiter
     * @return
     * @throws Exception
     */
    public static String parseIntegerArrayToString(Integer[] integers, String delimiter) throws Exception{
	StringBuffer parsed=new StringBuffer();
	//get the items from integers array 
	//populate the string with delimiters
	if(integers.length < 1)
	    return "";
	for(int index=0; index<integers.length; index++) {
	    //Append to the buffer
	    parsed.append(integers[index]);
	    if(index != (integers.length-1))
		parsed.append(delimiter);
	}

	return parsed.toString();
    }

    /**
     * Create a delimited string from the string array
     * @param strings
     * @param delimiter
     * @return
     * @throws Exception
     */
    public static String parseStringArrayToString(String[] strings, String delimiter) throws Exception{
	StringBuffer parsed=new StringBuffer();
	//get the items from strings array 
	//populate the string with delimiters
	if(strings == null || strings.length < 1)
	    return "";
	for(int index=0; index<strings.length; index++) {
	    //Append to the buffer
	    parsed.append(strings[index]);
	    if(index != (strings.length-1))
		parsed.append(delimiter);
	}

	return parsed.toString();
    }

    /**
     * Create a Integer array from a string with delimiters
     * @param integers
     * @param delimiter
     * @return
     * @throws Exception
     */
    public static Map<String,Integer[]> parseStringToIntegerArray(String integers, String delimiter) throws Exception{

	Map<String,Integer[]> parsedValues = new HashMap<String, Integer[]>();
	if(integers.length() < 1)
	    return parsedValues;
	String categories[] = integers.split("\\$");	
	for(String cat : categories){
	    String key = cat.substring(0, cat.indexOf("{"));
	    String values = cat.substring(cat.indexOf("{")+1,cat.indexOf("}"));
	    Integer idvalues[] = null;
	    if(values==null)
		parsedValues.put(key, idvalues);
	    else if(values.isEmpty())
		parsedValues.put(key, idvalues);
	    else{
		String ids[] = values.split(delimiter);
		idvalues = new Integer[ids.length];
		for(int i=0;i<ids.length;i++){
		    idvalues[i] = Integer.parseInt(ids[i]);
		}
		parsedValues.put(key, idvalues);
	    }
	}
	return parsedValues;
    }

    /**
     * Create a String array from a string with delimiters
     * @param strings
     * @param delimiter
     * @return
     * @throws Exception
     */
    public static  Map<String,String[]> parseStringToStringArray(String strings, String delimiter) throws Exception{

	Map<String,String[]> parsedValues = new HashMap<String, String[]>();
	if(strings.length() < 1)
	    return parsedValues;
	String categories[] = strings.split("\\$");	
	for(String cat : categories){
	    String key = cat.substring(0, cat.indexOf("{"));
	    String values = cat.substring(cat.indexOf("{")+1,cat.indexOf("}"));
	    String ids[] = null;
	    if(values==null)
		parsedValues.put(key, ids);
	    if(values.isEmpty())
		parsedValues.put(key, ids);
	    else{
		ids = values.split(delimiter);
		parsedValues.put(key, ids);
	    }
	}
	return parsedValues;
    }

    public static Integer[] parseStringArrayToIntegerArray(String[] strings) throws Exception{
	Integer idvalues[] = new Integer[strings.length];
	for(int i=0;i<strings.length;i++){
	    idvalues[i] = Integer.parseInt(strings[i]);
	}
	return idvalues;
    }

   
    /***
     *  Check array is empty or not
     * @param values
     * @return
     */
    public static boolean isValidCollection(Collection<?> collection){
	if(collection != null && !collection.isEmpty() && collection.size() > 0)
	    return true;
	return false;
    }

    /***
     *  Check array is empty or not
     * @param values
     * @return
     */
    public static boolean isValidMap(Map<?,?> mapObject){
	if(mapObject != null && !mapObject.isEmpty() && mapObject.size() > 0)
	    return true;
	return false;
    }

    /**
     * This method sorts a map by the values. Sorting is done on lexicographic order.
     * @param iMap
     * @return
     */
    public static LinkedHashMap<Integer, String> sortMapByValues(Map<Integer, String> iMap) {

	List<Entry<Integer,String>> sortedEntries = new ArrayList<Entry<Integer,String>>(iMap.entrySet());

	Collections.sort(sortedEntries, 
		new Comparator<Entry<Integer,String>>() {
	    @Override
	    public int compare(Entry<Integer,String> e1, Entry<Integer,String> e2) {
		return e1.getValue().compareTo(e2.getValue());
	    }
	}
		);

	LinkedHashMap<Integer, String> oMap = new LinkedHashMap<Integer, String>();
	for(Entry<Integer,String> e : sortedEntries) {
	    oMap.put(e.getKey(), e.getValue());
	}		
	return oMap;
    }

    /**
     * 
     * @param collection
     * @return
     */

    public static boolean checkListIsEmpty(Collection<Double> collection){
	if(isValidCollection(collection)){
	    for(Double doubleValue : collection){
		if(doubleValue > 0)
		    return false;
	    }
	}
	return true;
    }
    /**
     * Format the file path with the valid separators
     * @param filePath
     * @return
     */
    public static String formatFilePath(String filePath){
	if(filePath.isEmpty())
	    return "";
	filePath = filePath.replace("\\\\",File.separator);
	filePath = filePath.replace("\\",File.separator);
	filePath = filePath.replace("/",File.separator);
	return filePath;
    }
    /**
     * Replace function
     * @param str
     * @param pattern
     * @param replace
     * @return
     */
    public static String replace(String str, String pattern, String replace) {
	    int s = 0;
	    int e = 0;
	    StringBuffer result = new StringBuffer();
	
	    while ((e = str.indexOf(pattern, s)) >= 0) {
	        result.append(str.substring(s, e));
	        result.append(replace);
	        s = e + pattern.length();
	    }
	    result.append(str.substring(s));
	    return result.toString();
    }
    
    /**
     * Get the matched value
     * @param regex
     * @param text
     * @return
     */
    public static String findTextBasedOnRegex(String regex, String text) {
		List<String> result = null;
		Matcher m = Pattern.compile(regex).matcher(text);
		if (m.find()) {
			result = new ArrayList<String>();
			result.add(m.group());
			while (m.find()) {
				result.add(m.group());
			}
		}
		String val = "";
		if(result != null)
			val =result.get(0);
		return val;
	}
    
    
    /**
     * Get the SHA256 hashed value
     * @param value
     * @return
     * @throws NoSuchAlgorithmException
     */
    public static String getSHA256HashValue(String value) throws NoSuchAlgorithmException{
    	if(value == null || value.isEmpty()) return null;
    	//Create MessageDigest object for SHA256
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(value.getBytes(), 0, value.length());
        byte byteData[] = md.digest();
        //convert the byte to hex format method
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) {
         sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }
    
    
    /**
     * Get the MD5 hashed value
     * @param value
     * @return
     * @throws NoSuchAlgorithmException
     */
    public static String getMD5HashValue(String value) throws NoSuchAlgorithmException {
        if(value == null || value.isEmpty()) return null;
        String md5 = null;
        //Create MessageDigest object for MD5
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(value.getBytes(), 0, value.length());
        //Alternate method to Converts message digest value in base 16 (hex)
        md5 = new BigInteger(1, md.digest()).toString(16);
        return md5;
    }
    

    /**
     * Get the MD5 hashed value with an appended salt
     * @param value
     * @return
     * @throws NoSuchAlgorithmException
     */
    public static String getMD5HashValue(String value, String salt) throws NoSuchAlgorithmException {
        if(value == null || value.isEmpty()) return null;
        String md5 = null;
        value+=salt;
        //Create MessageDigest object for MD5
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(value.getBytes(), 0, value.length());
        //Alternate method to Converts message digest value in base 16 (hex)
        md5 = new BigInteger(1, md.digest()).toString(16);
        return md5;
    }
    
    /** Get String array values by given comma separated string
     * 
     * @param commaString
     * @return
     */
    public static String[] getStringArrayByGivenString(String commaString){
    	String[] arrayString = null;
    	if(Utility.isValidString(commaString)){
			if(commaString.contains(",")){
				arrayString = commaString.split(",");
			}else{
				arrayString = new String[]{commaString};  //new String[0];
				//arrayString[0] = commaString;
			}
		}
    	return arrayString;
    }
    
    /** Concate two string values
     * 
     * @param firstString
     * @param secondString
     * @param delim
     * @return
     */
    public static String concateString(String firstString,String secondString,String delim ){
    	String resultString = "";
		if(Utility.isValidString(firstString)){
			resultString = firstString;
		}
		if(Utility.isValidString(secondString)){
			resultString = resultString + delim +secondString;
		}
    	return resultString;
    }
    
    /** Concate two string values
     * 
     * @param firstString
     * @param secondString
     * @param delim
     * @return
     */
    public static String[] getFirstAndLastName(String name,String delim ){
    	String[] targetNames = new String[2];
    	if(name.contains(delim)){
    		String [] targetValues = name.split(delim);
    		String temp = "";
    		if(targetValues.length > 0){
    			for(int i=0;i<targetValues.length;i++){
    				if(i == 0){
    					targetNames[0] = targetValues[i];
    				}else{
    					if(targetValues[i] != null)
    						temp = temp + targetValues[i] +" ";
    				}
    			}
    			targetNames[1] = temp.trim();
    		}
    	}else{
    		targetNames[0] = name;
    	}
    	
    	return targetNames;
    }
    
    /**
     * Read string content of file
     * 
     * @param fullPath
     * @return
     * @throws Exception
     */
    public static String readStringFile(String fullPath) throws Exception{
    	/**
		 * Load the data from json file to a string
		 */
		String imageDataString = null;
		FileInputStream imageInFile = null;
		String fileInPath = fullPath; 
		try{
			log.info("Load file: "+fullPath);
			File file = new File(fileInPath);
			// Reading a Image file from file system
			imageInFile = new FileInputStream(file);
			byte imageData[] = new byte[(int) file.length()];
			imageInFile.read(imageData);
			imageDataString = new String(imageData);
		} catch (FileNotFoundException e) {
			log.error("Image not found" + e);
		} catch (IOException ioe) {
			log.error("Exception while reading the Image " + ioe);
		} finally{
			imageInFile.close();
		}
		return imageDataString;
    }
    
    /**
     * 
     * @param value
     * @return
     */
    public static boolean isStringValueZero(String value){
    	if(isValidString(value) && value.equalsIgnoreCase("0"))
    		return true;
    	return false;
    }
    
    
    /**
     * @param dateStr
     * @return
     */
    public static boolean verfiyDateFormat(String dateStr){
    	String pattern = new String("[\\d]{4}-[\\d]{2}-[\\d]{2}$");
        Pattern datePattern = Pattern.compile(pattern);
        Matcher dateMatcher = datePattern.matcher(dateStr);
        if(!dateMatcher.find()){
            System.out.println("Invalid date format!!! -> " + dateStr);
            return false;
        }
        System.out.println("Valid date format.");
        return true;
    } 
    
    /**
     * Decode String
     * 
     * @param s
     * @return
     */
    public static String decodeBase64(String s) {
        return StringUtils.newStringUtf8(Base64.decodeBase64(s));
    }
    
    /**
     * Encode String
     * 
     * @param s
     * @return
     */
    public static String encodeBase64(String s) {
        return Base64.encodeBase64String(StringUtils.getBytesUtf8(s));
    }
    
    /**
     * Encodes the byte array into base64 string
     *
     * @param imageByteArray - byte array
     * @return String a {@link java.lang.String}
     */
    public static String encodeImage(byte[] imageByteArray) {
        return Base64.encodeBase64URLSafeString(imageByteArray);
    }
 
    /**
     * Decodes the base64 string into byte array
     *
     * @param imageDataString - a {@link java.lang.String}
     * @return byte array
     */
    public static byte[] decodeImage(String imageDataString) {
        return Base64.decodeBase64(imageDataString);
    }
    
    /**
     * Encryption functions for data
     */
    private static final String ALGO = "AES";
    private static final byte[] keyValue = new byte[] {'W','S','A','L','T','Y','M','A','I','L','E','R','S'};
    
    public static String encrypt(String Data) throws Exception {
        Key key = generateKey();
        Cipher c = Cipher.getInstance(ALGO);
        c.init(Cipher.ENCRYPT_MODE, key);
        byte[] encVal = c.doFinal(Data.getBytes());
        String encryptedValue = new BASE64Encoder().encode(encVal);
        return encryptedValue;
    }
    
    public static String decrypt(String encryptedData) throws Exception {
        Key key = generateKey();
        Cipher c = Cipher.getInstance(ALGO);
        c.init(Cipher.DECRYPT_MODE, key);
        byte[] decordedValue = new BASE64Decoder().decodeBuffer(encryptedData);
        byte[] decValue = c.doFinal(decordedValue);
        String decryptedValue = new String(decValue);
        return decryptedValue;
    }
    
    private static Key generateKey() throws Exception {
        Key key = new SecretKeySpec(keyValue, ALGO);
        return key;
    }
    
    /**
     * @return
     */
    public static String getLocalHostName(){
    	String localhostname = "";
		try {
			localhostname = java.net.InetAddress.getLocalHost()
					.getCanonicalHostName();
		} catch (Exception e) {
			log.error("Unable to resolve hostname. " + localhostname);
		}
		if (localhostname == null || localhostname.isEmpty()) {
			try {
				localhostname = java.net.InetAddress.getLocalHost()
						.getHostAddress();
			} catch (Exception e) {
				log.error("Unable to resolve host adderess. " + localhostname);
			}
		}
		if (localhostname == null || localhostname.isEmpty()) {
			localhostname = "Server";
		}
    	return localhostname;
    }
    
}
