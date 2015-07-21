package jp.ac.nii;

import java.util.ArrayList;
import java.util.List;

public class Area {
	private String areaName;
	private List<Rank> rankList;

	public String getAreaName() {
		return areaName;
	}

	public void setAreaName(String areaName) {
		this.areaName = areaName;
	}

	public List<Rank> getRankList() {
		return rankList;
	}

	public void setRankList(List<Rank> rankList) {
		this.rankList = rankList;
	}

	public void addRankList(Rank rank) {
		if (rankList == null) {
			rankList = new ArrayList<Rank>();
		}
		rankList.add(rank);
	}
}
