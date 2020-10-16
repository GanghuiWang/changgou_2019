package com.changgou;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xpand.starter.canal.annotation.*;

/***
 * 实现MySQL数据监听
 */
@CanalEventListener
public class CanalDataEventListener {
    /***
     * 增加监听
     */
    @InsertListenPoint
    public void onEventInsert(CanalEntry.EventType eventType,CanalEntry.RowData rowData){
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            System.out.println("列名:" + column.getName() + "------变更的数据:" + column.getValue());
        }
    }
    /***
     * 修改监听
     */
    @UpdateListenPoint
    public void onEventUpdate(CanalEntry.EventType eventType,CanalEntry.RowData rowData){
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            System.out.println("修改前：列名:" + column.getName() + "------变更的数据:" + column.getValue());
        }

        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
            System.out.println("修改后：列名:" + column.getName() + "------变更的数据:" + column.getValue());
        }
    }
    /***
     * 删除监听
     */
    @DeleteListenPoint
    public void onEventDelete(CanalEntry.EventType eventType,CanalEntry.RowData rowData){
        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
            System.out.println("删除前：列名:" + column.getName() + "------变更的数据:" + column.getValue());
        }
    }
    /***
     * 自定义监听
     */
    @ListenPoint(eventType ={CanalEntry.EventType.DELETE, CanalEntry.EventType.UPDATE}, //指定监听的操作
            schema = {"changgou_content"},//指定监听的数据库
            table = {"tb_content"},//指定监控的表
            destination = "example" //指定实例的地址
    )
    public void onEventCustomUpdate(CanalEntry.EventType eventType,CanalEntry.RowData rowData){

        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
            System.out.println("====自定义操作前：列名:" + column.getName() + "------变更的数据:" + column.getValue());
        }
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            System.out.println("====自定义操作后：列名:" + column.getName() + "------变更的数据:" + column.getValue());
        }
    }
}
