package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  Iterator iter;
  Integer[] fields; 
  boolean open;
  Schema s;               //output schema
  public Projection(Iterator iter, Integer... fields) {
    //throw new UnsupportedOperationException("Not implemented");
    super.schema = iter.schema;
    this.iter = iter;
    this.fields = fields;
 	  open = true;
    //s = null;
    

    s = new Schema(fields.length);
    for( int i = 0; i < fields.length; i++)     //create new schema
    {
     s.initField(i, iter.schema, fields[i]);
    }
    super.schema = s;
      
    //System.arraycopy( fields, 0, this.fields, 0, fields.length );
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
    iter.restart();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    //throw new UnsupportedOperationException("Not implemented");
    return open;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    //throw new UnsupportedOperationException("Not implemented");
    open = false;
    iter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    //throw new UnsupportedOperationException("Not implemented");
    return iter.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    //throw new UnsupportedOperationException("Not implemented");
     Tuple t;
     
     Tuple out;
     if (open){
		while (iter.hasNext()){ //while loop was unnecessary, but I'm going to leave it alone
			try{
				t = this.iter.getNext();
				
				//outBytes = this.bScan.getNext(curRid);
			}catch (IllegalStateException e){
				throw new IllegalStateException();
			}

      out = new Tuple(s);
      for( int i = 0; i < fields.length; i++)     //construct new tuple
      {
       out.setField(i, t.getField(fields[i]));
      }
      return out;
		}
	}else{
		throw new IllegalStateException("iterator is closed");
	}
	throw new IllegalStateException("no more tuples");
  }

} // public class Projection extends Iterator
