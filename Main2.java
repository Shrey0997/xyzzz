import java.util.List;
import java.util.Comparator;
import java.util.stream.Collectors;

public class BostonHousing {
    public static void main(String[] args) throws InterruptedException {
// Read and parse the CSV file
        List<List<String>> data = readData("./bos-housing.csv");
// Data processing #1: Identify areas/blocks next to Charles river and compute statistics
        Thread t1 = new Thread(() -> {
            List<Double> charlesRiverPrices = data.stream()
                    .filter(area -> area.get(3).equals("\"1\"")) // filter by CHAS attribute
                    .map(area -> Double.parseDouble(area.get(13))) // extract MEDV attribute
                    .collect(Collectors.toList());
            double charlesRiverMax = charlesRiverPrices.stream().max(Comparator.naturalOrder()).orElse(0.0);
            double charlesRiverMin = charlesRiverPrices.stream().min(Comparator.naturalOrder()).orElse(0.0);
            double charlesRiverAvg = charlesRiverPrices.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            System.out.printf("Charles river areas/blocks: Max=%.2f, Min=%.2f, Avg=%.2f%n",
                    charlesRiverMax, charlesRiverMin, charlesRiverAvg);
        });

        // Data processing #2: Identify areas/blocks with low crime rate and low pupil-teacher ratio and compute statistics
        Thread t2 = new Thread(() -> {
            List<Double> lowCrimeLowPTRatioPrices = data.stream()
                    .sorted(Comparator.comparing(area -> Double.parseDouble(area.get(0)))) // sort by CRIM attribute
                    .sorted(Comparator.comparing(area -> Double.parseDouble(area.get(10)))) // sort by PTRATIO attribute
                    .limit(data.size() / 10) // select top (lowest) 10%
                    .map(area -> Double.parseDouble(area.get(13))) // extract MEDV attribute
                    .collect(Collectors.toList());
            List<Double> lowCrimeLowPTRatioNOX = data.stream()
                    .sorted(Comparator.comparing(area -> Double.parseDouble(area.get(0)))) // sort by CRIM attribute
                    .sorted(Comparator.comparing(area -> Double.parseDouble(area.get(10)))) // sort by PTRATIO attribute
                    .limit(data.size() / 10) // select top (lowest) 10%
                    .map(area -> Double.parseDouble(area.get(4))) // extract NOX attribute
                    .collect(Collectors.toList());
            List<Double> lowCrimeLowPTRatioRooms = data.stream()
                    .sorted(Comparator.comparing(area -> Double.parseDouble(area.get(0)))) // sort by CRIM attribute
                    .sorted(Comparator.comparing(area -> Double.parseDouble(area.get(10)))) // sort by PTRATIO attribute
                    .limit(data.size() / 10) // select top (lowest) 10%
                    .map(area -> Double.parseDouble(area.get(5))) // extract RM attribute
                    .collect(Collectors.toList());
            double lowCrimeLowPTRatioMaxPrice = lowCrimeLowPTRatioPrices.stream().max(Comparator.naturalOrder()).orElse(0.0);
            double lowCrimeLowPTRatioMinPrice = lowCrimeLowPTRatioPrices.stream().min(Comparator.naturalOrder()).orElse(0.0);
            double lowCrimeLowPTRatioAvgPrice = lowCrimeLowPTRatioPrices.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            System.out.printf("Low crime rate and low pupil-teacher ratio areas/blocks: Max=%.2f, Min=%.2f, Avg=%.2f%n",
                    lowCrimeLowPTRatioMaxPrice, lowCrimeLowPTRatioMinPrice, lowCrimeLowPTRatioAvgPrice);
            double lowCrimeLowPTRatioMaxNOX = lowCrimeLowPTRatioNOX.stream().max(Comparator.naturalOrder()).orElse(0.0);
            double lowCrimeLowPTRatioMinNOX = lowCrimeLowPTRatioNOX.stream().min(Comparator.naturalOrder()).orElse(0.0);
            double lowCrimeLowPTRatioAvgNOX = lowCrimeLowPTRatioNOX.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            System.out.printf("Low crime rate and low pupil-teacher ratio areas/blocks NOX levels: Max=%.2f, Min=%.2f, Avg=%.2f%n",
                    lowCrimeLowPTRatioMaxNOX, lowCrimeLowPTRatioMinNOX, lowCrimeLowPTRatioAvgNOX);
            double lowCrimeLowPTRatioMaxRooms = lowCrimeLowPTRatioRooms.stream().max(Comparator.naturalOrder()).orElse(0.0);
            double lowCrimeLowPTRatioMinRooms = lowCrimeLowPTRatioRooms.stream().min(Comparator.naturalOrder()).orElse(0.0);
            double lowCrimeLowPTRatioAvgRooms = lowCrimeLowPTRatioRooms.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            System.out.printf("Low crime rate and low pupil-teacher ratio areas/blocks average number of rooms: Max=%.2f, Min=%.2f, Avg=%.2f%n",
                    lowCrimeLowPTRatioMaxRooms, lowCrimeLowPTRatioMinRooms, lowCrimeLowPTRatioAvgRooms);
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    // Helper method to read the CSV file
    private static List<List<String>> readData(String file) {
        List<List<String>> data = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = reader.readLine(); // skip the header line
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                data.add(Arrays.asList(values));
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }
}

