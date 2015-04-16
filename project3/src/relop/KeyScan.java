package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import global.RID;
import index.HashScan;
/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {			///I AM USING BUCKET SCAN, CHANGE TO HEAP SCAN

  /**
   * Constructs an index scan, given the hash index and schema.
   */
   HeapFile file;
   Schema schema;
   HashScan hScan;
   HashIndex index;
   SearchKey key;
   boolean open;
	RID curRid;
	
  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
    //throw new UnsupportedOperationException("Not implemented");
    this.schema = schema;//schema shouldnt need to be manually copied, wont be destructed either 
    this.file = file;
    //index = new HashIndex(file.toString());			//do I need to initialize here?
    this.index = index;
    hScan = index.openScan(key);
    this.key = new SearchKey(key);
    open = true;
    curRid = new RID();
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    //throw new UnsupportedOperationException("Not implemented");
    this.explain(depth);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    //throw new UnsupportedOperationException("Not implemented");
        hScan.close();
    	hScan = index.openScan(this.key);
    	this.curRid = new RID();
    	open = true;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    //throw new UnsupportedOperationException("Not implemented");
    return this.open;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    //throw new UnsupportedOperationException("Not implemented");
       	// try {
    	hScan.close();
  		// }catch (Exception e){ 
    	open = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    //throw new UnsupportedOperationException("Not implemented");
    if (open){
    	return hScan.hasNext();
    }
    return false;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    //throw new UnsupportedOperationException("Not implemented");
    if (open){
		byte[] outBytes;
		try{
			curRid.copyRID(this.hScan.getNext());
			outBytes = new byte[curRid.getLength()];
			curRid.writeData(outBytes, (short) 0);
			//outBytes = this.bScan.getNext(curRid);
		}catch (IllegalStateException e){
			throw new IllegalStateException();
		}
		return new Tuple(this.schema, outBytes);
	}
    return null;	//???????????????????			or throw IllegalStateException?
  }
} // public class KeyScan extends Iterator
