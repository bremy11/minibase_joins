package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

  /**
   * Constructs a file scan, given the schema and heap file.
   */
   HeapFile file;
   Schema schema;
   HeapScan hScan;
   boolean open;
	RID curRid;
  public FileScan(Schema schema, HeapFile file) {
    //throw new UnsupportedOperationException("Not implemented");
    this.schema = schema;//schema shouldnt need to be manually copied, wont be destructed either 
    this.file = file;
    hScan = new HeapScan(file);
    open = true;
    curRid = new RID();
    //or 
    //this.file = new HeapFile(file.toString());
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    //throw new UnsupportedOperationException("Not implemented");
    this.explain(depth);				//check iterator class
    
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    //throw new UnsupportedOperationException("Not implemented");
    hScan.finalize();
    hScan = new HeapScan(file);
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
    	hScan.finalize();
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
			outBytes = this.hScan.getNext(curRid);
		}catch (IllegalStateException e){
			throw new IllegalStateException();
		}
		return new Tuple(this.schema, outBytes);
	}
    return null;	//???????????????????			or throw IllegalStateException?
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    //throw new UnsupportedOperationException("Not implemented");
    return curRid;
  }

} // public class FileScan extends Iterator