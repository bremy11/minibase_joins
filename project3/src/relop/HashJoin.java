package relop;

import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Hashtable;
import heap.HeapFile;
import global.RID;
import global.SearchKey;

/**
 * Implements the hash-based join algorithm described in section 14.4.3 of the
 * textbook (3rd edition; see pages 463 to 464). HashIndex is used to partition
 * the tuples into buckets, and HashTableDup is used to store a partition in
 * memory during the matching phase.
 */
public class HashJoin extends Iterator
{
    class TEntry{
        Object key;
        RID id;
        TEntry(Object key, RID id)
        {
            this.key = key;
            this.id = id;
        }
    }
    
    final int K = 100;
    
    Iterator left;
    Iterator right;
    Integer lcol;
    Integer rcol;
    boolean lfilescan = false;
    boolean rfilescan = false;
    boolean lindexscan = false;
    boolean rindexscan = false;
    
	boolean isOpen;
    LinkedList<TEntry> lpartitions[];
    LinkedList<TEntry> rpartitions[];
    HeapFile lheap;
    HeapFile rheap;
    
    int currentPart;
    HashTableDup lsingleparthash;
    ListIterator<HashJoin.TEntry> rIter;
    Tuple next;
    
	/**
	* Constructs a hash join, given the left and right iterators and which
	* columns to match (relative to their individual schemas).
	*/
	public HashJoin(Iterator left, Iterator right, Integer lcol, Integer rcol)
	{
        this.left = left;
        this.right = right;
        this.lcol = lcol;
        this.rcol = rcol;
		schema = Schema.join(left.schema, right.schema);
        lfilescan = left instanceof FileScan;
        rfilescan = right instanceof FileScan;
        lindexscan = left instanceof IndexScan;
        rindexscan = right instanceof IndexScan;
        
        lpartitions = new LinkedList[K];
        rpartitions = new LinkedList[K];
        
        if(lindexscan){
            lheap = ((IndexScan) left).file;
        }else if(lfilescan){
            lheap = ((FileScan) left).file;
        }else{
            lheap = new HeapFile(null); 
        }
        
        if(rindexscan){
            rheap = ((IndexScan) right).file;
        }else if(rfilescan){
            rheap = ((FileScan) right).file;
        }else{
            rheap = new HeapFile(null); 
        }
        
        for(int i = 0; i < K; i++)
        {
            lpartitions[i] = new LinkedList<TEntry>();
            rpartitions[i] = new LinkedList<TEntry>();
        }
        
        while(left.hasNext())
        {
            Tuple l = left.getNext();
            RID rid;
            
            if(lindexscan){
                rid = ((IndexScan) left).curRid;
            }else if(lfilescan){
                rid = ((FileScan) left).getLastRID();
            }else{
                rid = lheap.insertRecord(l.getData());
            }
            
            int lhashcode = getHash(l.getField(lcol));
            lpartitions[lhashcode].addLast(new TEntry(l.getField(lcol), rid));
        }
        
        while(right.hasNext())
        {
            Tuple r = right.getNext();
            RID rid;
            
            if(rindexscan){
                rid = ((IndexScan) right).curRid;
            }else if(rfilescan){
                rid = ((FileScan) right).getLastRID();
            }else{
                rid = rheap.insertRecord(r.getData());
            }
            
            int rhashcode = getHash(r.getField(rcol));
            rpartitions[rhashcode].addLast(new TEntry(r.getField(rcol), rid));
        }
        
        init();
	}
    
    private void init()
    {
        next = null;
        isOpen = true;
        currentPart = -1;
        lsingleparthash = null;
        rIter = null;
        
        setNext();
    }
    
    private int getHash(Object field)
    {
        return field.hashCode() % K;
    }

	/**
	* Gives a one-line explaination of the iterator, repeats the call on any
	* child iterators, and increases the indent depth along the way.
	*/
	public void explain(int depth)
	{
		indent(depth);
		System.out.println("HashJoin");
		left.explain(depth + 1);
		right.explain(depth + 1);
	}

	/**
	* Restarts the iterator, i.e. as if it were just constructed.
	*/
	public void restart()
	{
        left.restart();
        right.restart();
        init();
	}

	/**
	* Returns true if the iterator is open; false otherwise.
	*/
	public boolean isOpen()
	{
		return isOpen;
	}
    
    private void clean()
    {
        lheap = null;
        rheap = null;
    }

	/**
	* Closes the iterator, releasing any resources (i.e. pinned pages).
	*/
	public void close()
	{
        clean();
		isOpen = false;
		left.close();
		right.close();
	}

	/**
	* Returns true if there are more tuples, false otherwise.
	*/
	public boolean hasNext()
	{
		return next != null;
	}
    
    void nextPart()
    {
        currentPart++;
        lsingleparthash = null;
        
        if(currentPart < K)
        {
            rIter = rpartitions[currentPart].listIterator();
            
            lsingleparthash = new HashTableDup();
            for(TEntry ent : lpartitions[currentPart])
            {
                lsingleparthash.add(new SearchKey(ent.key), new Tuple(left.schema, lheap.selectRecord(ent.id)));
            }
        }
    }
    
    //dup control
    int dupSize = 0;
    int curDup = 0;
    Tuple[] dups;
    TEntry curEntry;
    
    void setNext()
    {
        next = null;
        
        if(curDup < dupSize)
        {
            Tuple l = dups[curDup++];
            Tuple r = new Tuple(right.schema, rheap.selectRecord(curEntry.id));
            Tuple out = Tuple.join(l, r, schema);
            next = out;
            return;
        }
        
        while(true)
        {
            while(currentPart == -1 || (!rIter.hasNext() && currentPart < K))
            {
                nextPart();
            }
            if(currentPart >= K) break;
            
            curEntry = rIter.next();
            
            dups = lsingleparthash.getAll(new SearchKey(curEntry.key));
            
            if(dups != null)
            {
                dupSize = dups.length;
                curDup = 1;
                
                Tuple l = dups[0];
                Tuple r = new Tuple(right.schema, rheap.selectRecord(curEntry.id));
                Tuple out = Tuple.join(l, r, schema);
                next = out;
                return;
            }
        }
    }
    
    /**
	* Gets the next tuple in the iteration.
	* 
	* @throws IllegalStateException if no more tuples
	*/
	public Tuple getNext()
	{
		if(isOpen)
		{
            if(!hasNext())
            {
                throw new IllegalStateException("no more tuples");
            }
        
            Tuple tempNext = next;
			setNext();
            return tempNext;
		}
		else
		{
			throw new IllegalStateException("iterator is closed");
		}
	}
}