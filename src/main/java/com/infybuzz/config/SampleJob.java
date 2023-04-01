package com.infybuzz.config;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import com.infybuzz.model.StudentCsv;
import com.infybuzz.model.StudentJdbc;
import com.infybuzz.model.StudentJson;
import com.infybuzz.model.StudentResponse;
import com.infybuzz.model.StudentXml;
import com.infybuzz.processor.FirstItemProcessor;
import com.infybuzz.reader.FirstItemReader;
//import com.infybuzz.service.StudentService;
import com.infybuzz.writer.FirstItemWriter;

@Configuration
public class SampleJob {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private FirstItemReader firstItemReader;

	@Autowired
	private FirstItemProcessor firstItemProcessor;

	@Autowired
	private FirstItemWriter firstItemWriter;

//	@Autowired
//	private StudentService studentService;

	// below line is to autowire datasource mentioned in app.prop
//	@Autowired
//	private DataSource datasource;

	// config whn 2 db's are in use
	@Bean
	@Primary
	@ConfigurationProperties(prefix = "spring.datasource")
	public DataSource dataSource() {
		return DataSourceBuilder.create().build();
	}

	@Bean
	@ConfigurationProperties(prefix = "spring.universitydatasource")
	public DataSource universitydataSource() {
		return DataSourceBuilder.create().build();
	}

	@Bean
	public Job chunkJob() {
		return jobBuilderFactory.get("Chunk Job").incrementer(new RunIdIncrementer()).start(firstChunkStep()).build();
	}

	private Step firstChunkStep() {
		return stepBuilderFactory.get("First Chunk Step").<StudentCsv, StudentCsv>chunk(3)
				// .reader(jdbcCursorItemReader())
				.reader(flatFileItemReader(null))
				// .reader(staxEventItemReader(null))
				// .reader(jsonItemReader(null))
				// .processor(firstItemProcessor)
				// .reader(itemReaderAdapter())
				//.writer(jdbcBatchItemWriter()).build();
		.writer(jdbcBatchItemWriter1()).build();
	}

	@StepScope
	@Bean
	public FlatFileItemReader<StudentCsv> flatFileItemReader(
			@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {
		FlatFileItemReader<StudentCsv> flatFileItemReader = new FlatFileItemReader<StudentCsv>();

		flatFileItemReader.setResource(fileSystemResource);
		// commented job because we ar passing the input file as job parameter below is
		// for hardcoded

		DefaultLineMapper<StudentCsv> defaultLineMapper = new DefaultLineMapper<StudentCsv>();

		DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
		delimitedLineTokenizer.setNames("ID", "First Name", "Last Name", "Email");
		//delimitedLineTokenizer.setDelimiter("|");
		defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
		BeanWrapperFieldSetMapper<StudentCsv> fieldSetMapper = new BeanWrapperFieldSetMapper<StudentCsv>();
		fieldSetMapper.setTargetType(StudentCsv.class);
		defaultLineMapper.setFieldSetMapper(fieldSetMapper);
		flatFileItemReader.setLineMapper(defaultLineMapper);

		flatFileItemReader.setLinesToSkip(1);

		return flatFileItemReader;
	}

	@StepScope
	@Bean
	public JsonItemReader<StudentJson> jsonItemReader(
			@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {
		JsonItemReader<StudentJson> jsonItemReader = new JsonItemReader<StudentJson>();
		jsonItemReader.setResource(fileSystemResource);
		jsonItemReader.setJsonObjectReader(new JacksonJsonObjectReader<>(StudentJson.class));
		jsonItemReader.setMaxItemCount(8);
		jsonItemReader.setCurrentItemCount(2);

		return jsonItemReader;

	}

	// this is not working properly please check again sometimes later
	@StepScope
	@Bean
	public StaxEventItemReader<StudentXml> staxEventItemReader(
			@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {
		StaxEventItemReader<StudentXml> staxEventItemReader = new StaxEventItemReader<StudentXml>();
		staxEventItemReader.setResource(fileSystemResource);
		staxEventItemReader.setFragmentRootElementName("student");
		// converting java to xml -> marshalling ,doing reverse is called unmarshalling
		staxEventItemReader.setUnmarshaller(new Jaxb2Marshaller() {
			{
				setClassesToBeBound(StudentXml.class);
			}
		});
		return staxEventItemReader;
	}

	public JdbcCursorItemReader<StudentJdbc> jdbcCursorItemReader() {
		JdbcCursorItemReader<StudentJdbc> jdbcCursorItemReader = new JdbcCursorItemReader<StudentJdbc>();
		jdbcCursorItemReader.setDataSource(universitydataSource());
		jdbcCursorItemReader.setSql("select  first_name as firstName, last_name as lastName, email from student");
		jdbcCursorItemReader
				.setSql("select id, first_name as firstName, last_name as lastName," + "email from student");
		jdbcCursorItemReader.setRowMapper(new BeanPropertyRowMapper<>() {
			{
				setMappedClass(StudentJdbc.class);
			}
		});
		return jdbcCursorItemReader;
	}

//	//for reading from json file 
//	public ItemReaderAdapter<StudentResponse> itemReaderAdapter() {
//		ItemReaderAdapter<StudentResponse> itemReaderAdapter = new ItemReaderAdapter<StudentResponse>();
//		itemReaderAdapter.setTargetObject(studentService);
//		itemReaderAdapter.setTargetMethod("getStudent");
//      itemReaderAdapter.setArguments(new Object[] {1L, "Test"});
//		return itemReaderAdapter;
//	}

	@StepScope
	@Bean
	public FlatFileItemWriter<StudentJdbc> flatFileItemWriter(
			@Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource) {
		FlatFileItemWriter<StudentJdbc> flatFileItemWriter = new FlatFileItemWriter<StudentJdbc>();

		flatFileItemWriter.setResource(fileSystemResource);

		flatFileItemWriter.setHeaderCallback(new FlatFileHeaderCallback() {
			@Override
			public void writeHeader(Writer writer) throws IOException {
				writer.write("Id,First Name,Last Name,Email");
			}
		});

		flatFileItemWriter.setLineAggregator(new DelimitedLineAggregator<StudentJdbc>() {
			{
				// setDelimiter("|");
				setFieldExtractor(new BeanWrapperFieldExtractor<StudentJdbc>() {
					{
						setNames(new String[] { "id", "firstName", "lastName", "email" });
					}
				});
			}
		});

		flatFileItemWriter.setFooterCallback(new FlatFileFooterCallback() {

			@Override
			public void writeFooter(Writer writer) throws IOException {
				writer.write("Created@ " + new Date());

			}
		});

		return flatFileItemWriter;
	}

	//jdbc itemwriter using sql querry
//	@Bean
//	public JdbcBatchItemWriter<StudentCsv> jdbcBatchItemWriter() {
//		System.out.println("inside jdbcBatchItemWriter() method");
//		JdbcBatchItemWriter<StudentCsv> jdbcBatchItemWriter = new JdbcBatchItemWriter<StudentCsv>();
//		jdbcBatchItemWriter.setDataSource(universitydataSource());
//		jdbcBatchItemWriter.setSql(
//				"insert into student(id,first_name,last_name,email)" + "values (:id, :firstName, :lastName, :email)");
//		jdbcBatchItemWriter
//				.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<StudentCsv>());
//
//		return jdbcBatchItemWriter;
//	}
	
	//jdbc itemwriter using sql prepared statement
	
	@Bean
	public JdbcBatchItemWriter<StudentCsv> jdbcBatchItemWriter1() {
		
		System.out.println("inside jdbcBatchItemWriter1() method");
		JdbcBatchItemWriter<StudentCsv> jdbcBatchItemWriter = new JdbcBatchItemWriter<StudentCsv>();
		jdbcBatchItemWriter.setDataSource(universitydataSource());
		jdbcBatchItemWriter.setSql(
				"insert into student(id,first_name,last_name,email)" + "values (?, ?, ?, ?)");
		jdbcBatchItemWriter
				.setItemPreparedStatementSetter(new ItemPreparedStatementSetter<StudentCsv>() {
					
					@Override
					public void setValues(StudentCsv item, PreparedStatement ps) throws SQLException {
					       ps.setLong(1, item.getId());
					       ps.setString(2, item.getFirstName());
					       ps.setString(3, item.getLastName());
					       ps.setString(4, item.getEmail());
					       
						
					}
				});
		return jdbcBatchItemWriter;
	}

}
