package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {

  /**
   * Constructs a join, given the left and right iterators and join predicates
   * (relative to the combined schema).
   */
   Iterator left;
   Iterator right;
   Predicate[] preds;
   boolean open;
   Schema joinSchema;

  public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
    //throw new UnsupportedOperationException("Not implemented");
    this.left = left;
    this.right = right;
    this.preds = preds;
    open = true;

    joinSchema = Schema.join(left.schema, right.schema);
    super.schema = joinSchema;
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
    right.restart();
    left.restart();
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
    right.close();
    left.close();
    open = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    //throw new UnsupportedOperationException("Not implemented");
    return left.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() 
  {
    //throw new UnsupportedOperationException("Not implemented");
    Tuple l;
    Tuple r;
    Tuple out;
    if (open){
      
  		while (left.hasNext())
      {
  			l = left.getNext();
  			while (right.hasNext()){
  				
  				try{
  					r = right.getNext();
  				
  					//outBytes = this.bScan.getNext(curRid);
  				}catch (IllegalStateException e){
  					throw new IllegalStateException();
  				}
          out = Tuple.join(l,r, joinSchema);
  				for( int i = 0; i < preds.length; i++)
  				{

            /*
  					if (!preds[i].evaluate(l) || !preds[i].evaluate(r))
  					{
  						break;		//exit for loop
  					}
  					if (i == preds.length-1)		//we got past last predicate
  					{
              
  						return Tuple.join(l,r, joinSchema);			//add schema for join
  					}*/
            if (preds[i].evaluate(out) )
            {
              return out;    //exit for loop
            }

  				}
  			}
  			right.restart();
  		}
  	}else{
  		throw new IllegalStateException("iterator is closed");
  	}
  	throw new IllegalStateException("no more tuples");
  }

} // public class SimpleJoin extends Iterator

