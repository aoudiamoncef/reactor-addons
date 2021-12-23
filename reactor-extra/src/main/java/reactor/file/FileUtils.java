/*
 * Copyright (c) 2018-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.file;

import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;


/**
 * Functions to help file .
 *
 * @author Moncef AOUDIA
 */
public class FileUtils {

	private FileUtils() {
	}

	public static Flux<String> lines(Path path) {
		return lines(path, StandardCharsets.UTF_8);
	}
	public static Flux<String> lines(Path path, Charset charset) {
		BufferedReader br = newBufferedReader(path, charset);
		return Flux.create(
				sink -> sink
						.onRequest(n -> {
							for (int i = 0; i < n; i++) {
								String line = readLine(br);
								if (line == null) {
									closeBufferedReader(br);
									sink.complete();
									break;
								} else {
									sink.next(line);
								}
							}
						})
						.onCancel(() -> closeBufferedReader(br))
						.onDispose(() -> closeBufferedReader(br)));
	}

	private static BufferedReader newBufferedReader(Path path, Charset charset) {
		try {
			return Files.newBufferedReader(path, charset);
		} catch (IOException e) {
			throw Exceptions.propagate(e);
		}
	}

	private static String readLine( BufferedReader br) {
		try {
			return br.readLine();
		} catch (IOException e) {
			throw Exceptions.propagate(e);
		}
	}

	private static void closeBufferedReader( BufferedReader br) {
		try {
			br.close();
		} catch (IOException e) {
			throw Exceptions.propagate(e);
		}
	}

	public static Flux<Boolean> deleteDirectory(final Path path, final boolean keepRoot) {
		return walk(path)
				.sort(Comparator.reverseOrder())
				.filter(currentPath -> !(currentPath.equals(path) && keepRoot))
				.map(Path::toFile)
				.map(File::delete);
	}

	/**
	 * Return a Flux that is populated with Path by walking the file tree rooted at a given starting file.
	 * The file tree is traversed depth-first.
	 * @param path Path
	 * @return Flux of Path
	 */
	public static Flux<Path> walk(Path path) {
		return Mono.fromSupplier(() -> path)
				.map(Path::toFile)
				.expandDeep(FileUtils::fileFlux)
				.map(file -> Paths.get(file.getPath()));
	}

	private static Flux<File> fileFlux( File file) {
		return Mono.justOrEmpty(file.listFiles())
				.map(Arrays::asList)
				.flatMapMany(Flux::fromIterable);
	}

}
