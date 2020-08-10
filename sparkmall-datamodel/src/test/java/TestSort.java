public class TestSort {
    public static void sort(int arr[],int low,int high){
        int i,j,temp,t;
        if(low>high){
            return;
        }
        i = low;
        j = high;
        //把temp当做基准
        temp = arr[low];

        while(i<j){
            //右边往左依次递减
            while(temp<=arr[j]&&i<j){
                j--;
            }
            //再看左边，往右递增
            while(temp>arr[i]&&i<j){
                i++;
            }
            //如果条件满足则交换
            if(i<j){
                t = arr[j];
                arr[j]=arr[i];
                arr[i]=t;
            }
        }
        arr[low] = arr[i];
        arr[i] = temp;
        sort(arr,low,j-1);
        sort(arr,j+1,high);
    }
}
