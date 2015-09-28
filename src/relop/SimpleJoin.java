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
	Tuple l = null;
    Tuple r = null;
    boolean hasnext;
   Tuple cur = null; 
   
  public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
    //throw new UnsupportedOperationException("Not implemented");
    this.left = left;
    this.right = right;
    this.preds = preds;
    open = true;
	hasnext = true;
    joinSchema = Schema.join(left.schema, right.schema);
    super.schema = joinSchema;
    this.setupNext();
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
    l = left.getNext();
    r = right.getNext();
    open = true;
    hasnext = true;
    cur = null;
    this.setupNext();
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
    return hasnext;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
   public void setupNext(){
   
    Tuple out;
    if ( !left.isOpen() || !right.isOpen()){
    	throw new IllegalStateException("iterator is closed");
    }	
    if (l == null)
    {
    	//left.restart();
    	
    		l = left.getNext();
    }
    if (r == null){
   		//System.out.println("HERE121!!!!");
    	//right.restart();
    	r = right.getNext();
    }
	boolean lcont;
	boolean rcont;
    if (open){
  		do{
  			do{
          		out = Tuple.join(l,r, joinSchema);
  				for( int i = 0; i < preds.length; i++)
  				{
				    if (preds[i].evaluate(out) )
				    {
				    	if (right.hasNext()){
				    		r = right.getNext();
				    		hasnext = true;
				    	}else{
				    		hasnext = false;
				    	}
				      	cur = out;    //exit for loop
				      	//System.out.println();
				      	//out.print();
				      	//System.out.println();
				      	
				      	return;
				    }
  				}
  				rcont = right.hasNext();
  				if (rcont){
	  				try{
						r = right.getNext();
					//outBytes = this.bScan.getNext(curRid);
	  				}catch (IllegalStateException e){
	  					throw new IllegalStateException("get next on right failed");
	  				}
  				}
  			}while (rcont);
  			right.restart();
  			lcont = left.hasNext();
  			if (lcont){
  				try{
  					l = left.getNext();
  				}catch (IllegalStateException e){
  					throw new IllegalStateException("get next on left failed");
  				} 
  			}
  			//System.out.println("l: "+l);
  		}while (lcont);
  	}else{
  		throw new IllegalStateException("iterator is closed");
  	}
  	hasnext = false;
}
   
   
   
  public Tuple getNext() 
  {
    //throw new UnsupportedOperationException("Not implemented");
    Tuple out;
    if (open){
        if(hasNext()){
            out = cur;
            this.setupNext();
            return out;
        }
    }else{
		throw new IllegalStateException("iterator is closed");
	}
	throw new IllegalStateException("no more tuples");
} // public class SimpleJoin extends Iterator
}