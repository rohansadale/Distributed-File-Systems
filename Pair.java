public class Pair
{
    private int first; //first member of pair
    private boolean second; //second member of pair

    public Pair(int first, boolean second) 
	{
        this.first = first;
        this.second = second;
    }

    public void setFirst(int first) 
	{
        this.first = first;
    }

    public void setSecond(boolean second) 
	{
        this.second = second;
    }

    public int getFirst() 
	{
        return first;
    }

    public boolean getSecond() 
	{
        return second;
    }
}
