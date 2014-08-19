package org.apache.hadoop.hive.mastiff;

import helma.xmlrpc.XmlRpcClient;
import helma.xmlrpc.XmlRpcException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Hashtable;
import java.util.Vector;

public class MastiffMetastoreClient {

  final static public String DEFAULT_PREFIX = "default.";

  private final String serverIp;

  public MastiffMetastoreClient(String ip) {
    this.serverIp = ip;
  }

  public Hashtable getMetadata(String table_name) {
    String tblName = parseName(table_name);

    try {
      XmlRpcClient client = new XmlRpcClient(serverIp);

      Vector params = new Vector();
      params.addElement(tblName);
      Object rs = client.execute("find_table", params);
      if (rs instanceof Hashtable) {
        Hashtable result = (Hashtable) rs;
        return result;
      } else {
        if (rs instanceof Boolean && Boolean.FALSE.equals(rs)) {
          return null;
        }
      }
    } catch (MalformedURLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (XmlRpcException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return null;
  }

  public boolean create_mmtable(String tableName, String columnsMap, String algorithm,
      String codingType) {

    try {
      XmlRpcClient client = new XmlRpcClient(serverIp);

      Vector params = new Vector();
      params.addElement(tableName);
      params.addElement(columnsMap);
      params.addElement(algorithm);
      params.addElement(codingType);

      Object result = client.execute("create_table", params);

      if (result instanceof Boolean && Boolean.TRUE.equals(result)) {
        return true;
      } else if (result instanceof Boolean && Boolean.FALSE.equals(result)) {
        return false;
      }

    } catch (MalformedURLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (XmlRpcException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }

  public void delete_mmtable(String tableName) {
    try {
      XmlRpcClient client = new XmlRpcClient(serverIp);

      Vector params = new Vector();
      params.addElement(tableName);

      client.execute("delete_table", params);
    } catch (MalformedURLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (XmlRpcException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  public String parseName(String name) {
    if (name.startsWith(DEFAULT_PREFIX)) {
      name = name.substring(DEFAULT_PREFIX.length());
    }
    return name;
  }
}
