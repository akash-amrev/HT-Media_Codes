package org.htmedia.shine.simprofileknn;

import java.util.Arrays;

/**
 * Hello world!
 *
 */
public class App 
{
	private static float[] weightSim = new float[7];//{0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
	public static float[] expWiseWeight(float exp_years) {
		float[] weight = new float[7];
		float sumWeight = 0;
		weight[0] = (float) (0.05 + 0.1 / (1 + Math.exp(-1
				* (0.75 * Math.exp(0.23 * exp_years) - 3)))); // bounded
																// function
																// between
																// 5% and
																// 15%
		weight[1] = (float) (0.05 + 0.05 / (1 + Math.exp(-1
				* (0.75 * Math.exp(0.23 * exp_years) - 3)))); // bounded
																// function
																// between
																// 5% and
																// 10%
		weight[2] = (float) (0.05 + 0.15 / (1 + Math.exp(-1
				* (0.55 * Math.exp(0.2 * exp_years) - 3)))); // bounded
																// function
																// between
																// 5% and
																// 20%
		weight[3] = (float) (0.15 + 0.10 / (1 + Math.exp(-1
				* (0.55 * Math.exp(0.15 * exp_years) - 3)))); // bounded
																// function
																// between
																// 15% and
																// 25%
		weight[4] = (float) (0.05 + 0.1 / (1 + Math.exp(1 * (0.85 * Math
				.exp(0.3 * exp_years) - 3)))); // bunded function between 5%
												// and 15%
		weight[5] = (float) (0.14 + 0.02 / (1 + Math.exp(1 * (0.45 * Math
				.exp(0.1 * exp_years) - 3)))); // bounded function between
												// 10% and 15%
		weight[6] = (float) (0.30 + 0.10 / (1 + Math.exp(1 * (0.55 * Math
				.exp(0.2 * exp_years) - 3)))); // bounded function between
												// 30% and 40%
		for (int i = 0; i < 7; i++) {
			sumWeight += weight[i];
		}
		for (int i = 0; i < 7; i++) {
			weight[i] = weight[i] / sumWeight;
		}
		System.out.println(weight[6]);
		sumWeight = 0.0f;
		for (int i = 0; i < 7; i++) {
			sumWeight += weight[i];
		}
		System.out.println(sumWeight);
		return weight;
	}
	
    public static void main( String[] args )
    {
    	weightSim = expWiseWeight(5.25f);
    	System.out.println(Arrays.toString(weightSim));
    	weightSim = expWiseWeight(4.08f);
    	System.out.println(Arrays.toString(weightSim));

    }
}
