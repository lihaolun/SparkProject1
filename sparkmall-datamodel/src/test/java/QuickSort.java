public class QuickSort {


        public static void sort(int arr[], int low, int high) {
            //设置两个变量l、h，排序开始的时候：l=0，h=N-1
            int l = low;
            int h = high;
            //以第一个数组元素作为关键数据，赋值给key，即key=A[0]
            int key = arr[low];
            //重复操作，直到i=j
            while (l < h) {
                //从h开始向前搜索，即由后开始向前搜索(h--)，找到第一个小于key的值arr[h]，将arr[h]和arr[l]互换
                while (l < h && arr[h] >= key)
                    h--;
                if (l < h) {
                    int temp = arr[h];
                    arr[h] = arr[l];
                    arr[l] = temp;
                    l++;
                }
                //从l开始向后搜索，即由前开始向后搜索(l++)，找到第一个大于key的arr[l]，将arr[l]和arr[h]互换；
                while (l < h && arr[l] <= key)
                    l++;

                if (l < h) {
                    int temp = arr[h];
                    arr[h] = arr[l];
                    arr[l] = temp;
                    h--;
                }
            }
            //对前一部分进行快速排序
            if (l > low)
                sort(arr, low, l - 1);
            //对前一部分进行快速排序
            if (h < high)
                sort(arr, l + 1, high);
        }


}
