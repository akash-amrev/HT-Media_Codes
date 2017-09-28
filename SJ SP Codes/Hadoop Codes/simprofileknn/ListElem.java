package org.htmedia.shine.simprofileknn;

class ListElem {
	private int id;
	private float dist;
	private String simVals;

	ListElem(int dimension, float value, int id, String simVals) {
		this.dist = value;
		this.id = id;
		this.simVals = simVals;
	}

	ListElem(int dimension, float value) {
		this.dist = value;
	}

	void setDist(float value) {
		this.dist = value;
	}

	float getDist() {
		return this.dist;
	}

	int getId() {
		return this.id;
	}

	String getSimVals() {
		return this.simVals;
	}

	void setSimVals(String simVals) {
		this.simVals = simVals;
	}

	public String toString() {
		return Integer.toString(this.id) + " " + Float.toString(this.dist)
				+ " " + this.simVals;
	}
}
