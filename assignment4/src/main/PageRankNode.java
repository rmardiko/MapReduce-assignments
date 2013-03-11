/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */



import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayOfFloatsWritable;

/**
 * Representation of a graph node for PageRank. 
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class PageRankNode implements Writable {
  public static enum Type {
    Complete((byte) 0),  // PageRank mass and adjacency list.
    Mass((byte) 1),      // PageRank mass only.
    Structure((byte) 2); // Adjacency list only.

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

	private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

	private Type type;
	private int nodeid;
	private float pagerank;
	private ArrayListOfIntsWritable adjacencyList;
	
	// extension of the original code, to accomodate multi-source
	// personalized page rank
	private int numSources;
	private ArrayOfFloatsWritable pagerankVals;
	private ArrayListOfIntsWritable sources;

	public PageRankNode() {}

	public float getPageRank() {
		return pagerank;
	}

	public void setPageRank(float p) {
		this.pagerank = p;
	}
	
	public float getPersonalizedPageRank(int sourceId) {
	  
	  return pagerankVals.get(getSourceIndex(sourceId));
	}
	
	public void setPersonalizedPageRank(int sourceId, float value) {
	  pagerankVals.set(getSourceIndex(sourceId), value);
	}
	
	private int getSourceIndex(int source) {
	  boolean go = true; int index = -1;
	  for (int ii = 0; ii < sources.size() && go; ii++) {
	    if (sources.get(ii) == source) {
	      index = ii;
	      go = false;
	    }
	  }
	  
	  return index;
	}

	public int getNodeId() {
		return nodeid;
	}

	public void setNodeId(int n) {
		this.nodeid = n;
	}

	public ArrayListOfIntsWritable getAdjacencyList() {
		return adjacencyList;
	}

	public void setAdjacencyList(ArrayListOfIntsWritable list) {
		this.adjacencyList = list;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public int getNumSources() {
    return numSources;
  }

  public void setNumSources(int numSources) {
    this.numSources = numSources;
  }

  public void setSourceIds(ArrayListOfIntsWritable sourceIds) {
    
    sources = sourceIds;
    
    if (pagerankVals == null) {
      pagerankVals = new ArrayOfFloatsWritable();
      pagerankVals.setArray(new float[sourceIds.size()]);
    }
    
    
    for (int e : sourceIds) {
      pagerankVals.set(getSourceIndex(e),Float.NEGATIVE_INFINITY);
    }
    
    this.numSources = sources.size();
  }

  /**
	 * Deserializes this object.
	 *
	 * @param in source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int b = in.readByte();
		type = mapping[b];
		nodeid = in.readInt();

		if (type.equals(Type.Mass)) {
			pagerank = in.readFloat();
			
			sources = new ArrayListOfIntsWritable();
      sources.readFields(in);
      
      pagerankVals = new ArrayOfFloatsWritable();
      pagerankVals.readFields(in);
      
			return;
		}

		if (type.equals(Type.Complete)) {
			pagerank = in.readFloat();
			
			sources = new ArrayListOfIntsWritable();
			sources.readFields(in);
			
			pagerankVals = new ArrayOfFloatsWritable();
			pagerankVals.readFields(in);
	    
	    numSources = pagerankVals.size();
		}

		adjacencyList = new ArrayListOfIntsWritable();
		adjacencyList.readFields(in);
	}

	/**
	 * Serializes this object.
	 *
	 * @param out where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(type.val);
		out.writeInt(nodeid);

		if (type.equals(Type.Mass)) {
			out.writeFloat(pagerank);
			sources.write(out);
			pagerankVals.write(out);
			
			return;
		}

		if (type.equals(Type.Complete)) {
			out.writeFloat(pagerank);
			sources.write(out);
			pagerankVals.write(out);
		}
		
		adjacencyList.write(out);
	}

	@Override
	public String toString() {
		return String.format("{%d %.4f %s %s %s}",
				nodeid, pagerank, sources, (pagerankVals == null ? "[]" : pagerankVals.toString()),
				(adjacencyList == null ? "[]" : adjacencyList.toString(10)));
	}


  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNode create(DataInput in) throws IOException {
    PageRankNode m = new PageRankNode();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNode create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
