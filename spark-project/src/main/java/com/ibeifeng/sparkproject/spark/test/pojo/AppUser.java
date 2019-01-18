package com.ibeifeng.sparkproject.spark.test.pojo;

import java.io.Serializable;

public class AppUser implements Serializable {

  private String id;
  private String createTime;
  private String isDelete;
  private String remark;
  private String updateTime;
  private String headPortrait;
  private String identificationCards;
  private String realName;
  private String rongyunToken;
  private String sex;
  private String token;
  private String baseUserId;
  private String userIntegral;
  private String weixinOpenId;
  private String recommendCode;
  private String payNum;
  private String status;
  private String companyId;
  private String inviteCode;
  private String busIntegralId;

    @Override
    public String toString() {
        return "AppUser{" +
                "id=" + id +
                ", createTime='" + createTime + '\'' +
                ", isDelete='" + isDelete + '\'' +
                ", remark='" + remark + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", headPortrait='" + headPortrait + '\'' +
                ", identificationCards='" + identificationCards + '\'' +
                ", realName='" + realName + '\'' +
                ", rongyunToken='" + rongyunToken + '\'' +
                ", sex='" + sex + '\'' +
                ", token='" + token + '\'' +
                ", baseUserId='" + baseUserId + '\'' +
                ", userIntegral='" + userIntegral + '\'' +
                ", weixinOpenId='" + weixinOpenId + '\'' +
                ", recommendCode='" + recommendCode + '\'' +
                ", payNum='" + payNum + '\'' +
                ", status='" + status + '\'' +
                ", companyId='" + companyId + '\'' +
                ", inviteCode='" + inviteCode + '\'' +
                ", busIntegralId='" + busIntegralId + '\'' +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getIsDelete() {
        return isDelete;
    }

    public void setIsDelete(String isDelete) {
        this.isDelete = isDelete;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
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

    public String getBaseUserId() {
        return baseUserId;
    }

    public void setBaseUserId(String baseUserId) {
        this.baseUserId = baseUserId;
    }

    public String getUserIntegral() {
        return userIntegral;
    }

    public void setUserIntegral(String userIntegral) {
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

    public String getPayNum() {
        return payNum;
    }

    public void setPayNum(String payNum) {
        this.payNum = payNum;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getCompanyId() {
        return companyId;
    }

    public void setCompanyId(String companyId) {
        this.companyId = companyId;
    }

    public String getInviteCode() {
        return inviteCode;
    }

    public void setInviteCode(String inviteCode) {
        this.inviteCode = inviteCode;
    }

    public String getBusIntegralId() {
        return busIntegralId;
    }

    public void setBusIntegralId(String busIntegralId) {
        this.busIntegralId = busIntegralId;
    }
}
