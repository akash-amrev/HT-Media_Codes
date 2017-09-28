package org.htmedia.shine.simprofileknn;
import java.util.Comparator;

class RecordComparator implements Comparator<ListElem>
{
	public int compare(ListElem o1, ListElem o2) 
	{
		int ret = 0;

		double dist = o2.getDist() - o1.getDist();
		if (Math.abs(dist) < 1E-6) {
			//ret = 0;
			ret = o1.getId() - o2.getId();	
		} else if (dist > 0)
			ret = 1;
		else if (dist < 0)
			ret = -1;

		return -ret;  //Descending order
	}
}
