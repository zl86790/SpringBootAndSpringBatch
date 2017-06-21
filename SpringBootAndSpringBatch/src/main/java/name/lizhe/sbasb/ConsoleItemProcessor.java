package name.lizhe.sbasb;

import org.springframework.batch.item.ItemProcessor;

public class ConsoleItemProcessor implements ItemProcessor<String,String>{

	@Override
	public String process(String line) throws Exception {
		System.out.println(line);
		return line;
	}
}
