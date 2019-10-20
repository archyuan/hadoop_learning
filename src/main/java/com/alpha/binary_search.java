package com.alpha;

public class binary_search {
    public int search(int[] nums, int target) {
        int low = 0;
        int high = nums.length-1;
        int mid = low + ((high - low)>>1);
        while(low<=high){
            if(nums[low]==target)
                return low;
            if(nums[mid]==target)
                return mid;
            if(nums[high]==target)
                return high;
            if(nums[low]>nums[high]&&nums[low]>target)
                low = mid +1;
            else if(nums[low]>nums[high]&&nums[low]<target)
                high = mid - 1;
            else if(nums[low]<nums[high]&&nums[mid]>target)
                high = mid -1;
            else if(nums[low]<nums[high]&&nums[mid]<target)
                low = mid + 1;
            else if(nums[low]==nums[high]&&nums[low]!=target)
                break;
            mid = low + ((high - low)>>1);

        }

        return -1;
    }

    public static void main(String[] args) {
        binary_search binary_search = new binary_search();
        int[] nums = {4,5,6,7,8,1,2,3};
        int res = binary_search.search(nums,8);
        System.out.println(res);
    }
}
