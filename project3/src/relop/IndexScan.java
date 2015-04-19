package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import global.RID;
import index.BucketScan;
/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

  /**
   * Constructs an index scan, given the hash index and schema.
   */
   HeapFile file;
   Schema schema;
   BucketScan bScan;
   HashIndex index;
   boolean open;
	RID curRid;
	
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
    //throw new UnsupportedOperationException("Not implemented");
    this.schema = schema;//schema shouldnt need to be manually copied, wont be destructed either 
    super.schema = schema;
    this.file = file;
    //index = new HashIndex(file.toString());			//do I need to initialize here?
    this.index = index;
    bScan = index.openScan();
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
        bScan.close();
    	bScan = index.openScan();
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
    	bScan.close();
  		// }catch (Exception e){ 
    	open = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    //throw new UnsupportedOperationException("Not implemented");
    if (open){
    	return bScan.hasNext();
    }
    return false;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if (open){
		byte[] outBytes;
		try{
			curRid.copyRID(this.bScan.getNext());
			outBytes = this.file.selectRecord(curRid);
			//outBytes = this.bScan.getNext(curRid);
		}catch (IllegalStateException e){
			throw new IllegalStateException();
		}
		return new Tuple(this.schema, outBytes);
	}
    return null;	//???????????????????			or throw IllegalStateException?
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
    //throw new UnsupportedOperationException("Not implemented");
    return bScan.getLastKey();
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
    //throw new UnsupportedOperationException("Not implemented");
    return bScan.getNextHash();
  }

} // public class IndexScan extends Iterator
