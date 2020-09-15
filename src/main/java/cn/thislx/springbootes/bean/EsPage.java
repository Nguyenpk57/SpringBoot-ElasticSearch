package cn.thislx.springbootes.bean;

import lombok.Data;
import lombok.ToString;

import java.util.List;
import java.util.Map;

/**
 * @Author: LX
 * @Description:
 * @Date: Created in 11:23 2018/11/6
 * @Modified by:
 */
@Data
@ToString
public class EsPage {

    /**
     * current page
     */
    private int currentPage;
    /**
     * How many bars per page
     */
    private int pageSize;

    /**
     * total
     */
    private int recordCount;
    /**
     * Data list for this page
     */
    private List<Map<String, Object>> recordList;

    /**
     * total pages
     */
    private int pageCount;
    /**
     * Start index of page number list (inclusive)
     */
    private int beginPageIndex;
    /**
     * End index of page number list (inclusive)
     */
    private int endPageIndex;

    /**
     * Only accept the first 4 necessary attributes, and automatically calculate the value of the other 3 attributes
     *
     * @param currentPage
     * @param pageSize
     * @param recordCount
     * @param recordList
     */
    public EsPage(int currentPage, int pageSize, int recordCount, List<Map<String, Object>> recordList) {
        this.currentPage = currentPage;
        this.pageSize = pageSize;
        this.recordCount = recordCount;
        this.recordList = recordList;

        // Calculate total page number
        pageCount = (recordCount + pageSize - 1) / pageSize;

        // Calculate beginPageIndex and endPageIndex
        // >> Total number of pages is less than 10 pages
        if (pageCount <= 10) {
            beginPageIndex = 1;
            endPageIndex = pageCount;
        }
        // If the total number of pages is more than 10 pages, a total of 10 page numbers are displayed near the current page
        else {
            // A total of 10 page numbers near the current page (first 4 + current page + next 5)
            beginPageIndex = currentPage - 4;
            endPageIndex = currentPage + 5;
            // When the previous page number is less than 4, the first 10 page numbers are displayed
            if (beginPageIndex < 1) {
                beginPageIndex = 1;
                endPageIndex = 10;
            }
            // When there are less than 5 pages, the next 10 pages will be displayed
            if (endPageIndex > pageCount) {
                endPageIndex = pageCount;
                beginPageIndex = pageCount - 10 + 1;
            }
        }
    }
}
