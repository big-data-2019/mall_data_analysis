package com.ibeifeng.sparkproject.spark.test.pojo;


public class PaymentList {

  private Long id;
  private java.sql.Timestamp createTime;
  private Long isDelete;
  private String remark;
  private java.sql.Timestamp updateTime;
  private Double amount;
  private String orderType;
  private String outTradeNo;
  private String paySerialNumber;
  private String paySuccess;
  private String payType;
  private Long companyId;
  private Long payUser;
  private java.sql.Timestamp payTime;
  private String refund;
  private java.sql.Timestamp refundTime;
  private Double serviceFee;
  private String payBillNo;
  private String tpayType;

  @Override
  public String toString() {
    return "PaymentList{" +
            "id=" + id +
            ", createTime=" + createTime +
            ", isDelete=" + isDelete +
            ", remark='" + remark + '\'' +
            ", updateTime=" + updateTime +
            ", amount=" + amount +
            ", orderType='" + orderType + '\'' +
            ", outTradeNo='" + outTradeNo + '\'' +
            ", paySerialNumber='" + paySerialNumber + '\'' +
            ", paySuccess='" + paySuccess + '\'' +
            ", payType='" + payType + '\'' +
            ", companyId=" + companyId +
            ", payUser=" + payUser +
            ", payTime=" + payTime +
            ", refund='" + refund + '\'' +
            ", refundTime=" + refundTime +
            ", serviceFee=" + serviceFee +
            ", payBillNo='" + payBillNo + '\'' +
            ", tpayType='" + tpayType + '\'' +
            '}';
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }


  public java.sql.Timestamp getCreateTime() {
    return createTime;
  }

  public void setCreateTime(java.sql.Timestamp createTime) {
    this.createTime = createTime;
  }


  public Long getIsDelete() {
    return isDelete;
  }

  public void setIsDelete(Long isDelete) {
    this.isDelete = isDelete;
  }


  public String getRemark() {
    return remark;
  }

  public void setRemark(String remark) {
    this.remark = remark;
  }


  public java.sql.Timestamp getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(java.sql.Timestamp updateTime) {
    this.updateTime = updateTime;
  }


  public Double getAmount() {
    return amount;
  }

  public void setAmount(Double amount) {
    this.amount = amount;
  }


  public String getOrderType() {
    return orderType;
  }

  public void setOrderType(String orderType) {
    this.orderType = orderType;
  }


  public String getOutTradeNo() {
    return outTradeNo;
  }

  public void setOutTradeNo(String outTradeNo) {
    this.outTradeNo = outTradeNo;
  }


  public String getPaySerialNumber() {
    return paySerialNumber;
  }

  public void setPaySerialNumber(String paySerialNumber) {
    this.paySerialNumber = paySerialNumber;
  }


  public String getPaySuccess() {
    return paySuccess;
  }

  public void setPaySuccess(String paySuccess) {
    this.paySuccess = paySuccess;
  }


  public String getPayType() {
    return payType;
  }

  public void setPayType(String payType) {
    this.payType = payType;
  }


  public Long getCompanyId() {
    return companyId;
  }

  public void setCompanyId(Long companyId) {
    this.companyId = companyId;
  }


  public Long getPayUser() {
    return payUser;
  }

  public void setPayUser(Long payUser) {
    this.payUser = payUser;
  }


  public java.sql.Timestamp getPayTime() {
    return payTime;
  }

  public void setPayTime(java.sql.Timestamp payTime) {
    this.payTime = payTime;
  }


  public String getRefund() {
    return refund;
  }

  public void setRefund(String refund) {
    this.refund = refund;
  }


  public java.sql.Timestamp getRefundTime() {
    return refundTime;
  }

  public void setRefundTime(java.sql.Timestamp refundTime) {
    this.refundTime = refundTime;
  }


  public Double getServiceFee() {
    return serviceFee;
  }

  public void setServiceFee(Double serviceFee) {
    this.serviceFee = serviceFee;
  }


  public String getPayBillNo() {
    return payBillNo;
  }

  public void setPayBillNo(String payBillNo) {
    this.payBillNo = payBillNo;
  }


  public String getTpayType() {
    return tpayType;
  }

  public void setTpayType(String tpayType) {
    this.tpayType = tpayType;
  }

}
