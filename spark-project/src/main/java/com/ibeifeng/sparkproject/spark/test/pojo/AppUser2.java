package com.ibeifeng.sparkproject.spark.test.pojo;

public class AppUser2 {

  private Long id;
  private java.sql.Timestamp createTime;
  private Long isDelete;
  private String remark;
  private java.sql.Timestamp updateTime;
  private String headPortrait;
  private String identificationCards;
  private String realName;
  private String rongyunToken;
  private String sex;
  private String token;
  private Long baseUserId;
  private Long userIntegral;
  private String weixinOpenId;
  private String recommendCode;
  private Long payNum;
  private String status;
  private Long companyId;
  private String inviteCode;
  private Long busIntegralId;

    @Override
    public String toString() {
        return "AppUser2{" +
                "id=" + id +
                ", createTime=" + createTime +
                ", isDelete=" + isDelete +
                ", remark='" + remark + '\'' +
                ", updateTime=" + updateTime +
                ", headPortrait='" + headPortrait + '\'' +
                ", identificationCards='" + identificationCards + '\'' +
                ", realName='" + realName + '\'' +
                ", rongyunToken='" + rongyunToken + '\'' +
                ", sex='" + sex + '\'' +
                ", token='" + token + '\'' +
                ", baseUserId=" + baseUserId +
                ", userIntegral=" + userIntegral +
                ", weixinOpenId='" + weixinOpenId + '\'' +
                ", recommendCode='" + recommendCode + '\'' +
                ", payNum=" + payNum +
                ", status='" + status + '\'' +
                ", companyId=" + companyId +
                ", inviteCode='" + inviteCode + '\'' +
                ", busIntegralId=" + busIntegralId +
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


  public String getHeadPortrait() {
    return headPortrait;
  }

  public void setHeadPortrait(String headPortrait) {
    this.headPortrait = headPortrait;
  }


  public String getIdentificationCards() {
    return identificationCards;
  }

  public void setIdentificationCards(String identificationCards) {
    this.identificationCards = identificationCards;
  }


  public String getRealName() {
    return realName;
  }

  public void setRealName(String realName) {
    this.realName = realName;
  }


  public String getRongyunToken() {
    return rongyunToken;
  }

  public void setRongyunToken(String rongyunToken) {
    this.rongyunToken = rongyunToken;
  }


  public String getSex() {
    return sex;
  }

  public void setSex(String sex) {
    this.sex = sex;
  }


  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }


  public Long getBaseUserId() {
    return baseUserId;
  }

  public void setBaseUserId(Long baseUserId) {
    this.baseUserId = baseUserId;
  }


  public Long getUserIntegral() {
    return userIntegral;
  }

  public void setUserIntegral(Long userIntegral) {
    this.userIntegral = userIntegral;
  }


  public String getWeixinOpenId() {
    return weixinOpenId;
  }

  public void setWeixinOpenId(String weixinOpenId) {
    this.weixinOpenId = weixinOpenId;
  }


  public String getRecommendCode() {
    return recommendCode;
  }

  public void setRecommendCode(String recommendCode) {
    this.recommendCode = recommendCode;
  }


  public Long getPayNum() {
    return payNum;
  }

  public void setPayNum(Long payNum) {
    this.payNum = payNum;
  }


  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }


  public Long getCompanyId() {
    return companyId;
  }

  public void setCompanyId(Long companyId) {
    this.companyId = companyId;
  }


  public String getInviteCode() {
    return inviteCode;
  }

  public void setInviteCode(String inviteCode) {
    this.inviteCode = inviteCode;
  }


  public Long getBusIntegralId() {
    return busIntegralId;
  }

  public void setBusIntegralId(Long busIntegralId) {
    this.busIntegralId = busIntegralId;
  }

}
