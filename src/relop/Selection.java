package relop;


/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
   boolean open;
   Iterator iter;
   Predicate[] preds;
   Tuple cur = null;
   
  public Selection(Iterator iter, Predicate... preds) {
    //throw new UnsupportedOperationException("Not implemented");
    super.schema = iter.schema;       
    this.open = true;
    this.iter = iter; 	//is there a better way to copy the itterator?????
    iter.restart();
    this.preds = preds;
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
    iter.restart();
    open = true;
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
    iter.close();
    this.open = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    //throw new UnsupportedOperationException("Not implemented");
     return cur != null;
  }
  
    private void setupNext() {
        Tuple t;
        
        while (iter.hasNext()){
            try{
                t = this.iter.getNext();
                //t.print();
                //outBytes = this.bScan.getNext(curRid);
            }catch (IllegalStateException e){
                throw new IllegalStateException();
            }
            for(int i = 0; i < preds.length; i++)
            {
                if (preds[i].evaluate(t))
                {
                    cur = t; //exit for loop
                    return;
                }
            }
        }
        
        cur = null;
    }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    //throw new UnsupportedOperationException("Not implemented");
    Tuple t;
    if (open){
        if(hasNext())
        {
            Tuple out = cur;
            this.setupNext();
            return out;
        }
	}else{
		throw new IllegalStateException("iterator is closed");
	}
	throw new IllegalStateException("no more tuples");
    //return null;	//???????????????????			or throw IllegalStateException?
  }

} // public class Selection extends Iterator
