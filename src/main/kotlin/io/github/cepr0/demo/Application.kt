package io.github.cepr0.demo

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cache.CacheManager
import org.springframework.cache.annotation.Cacheable
import org.springframework.cache.annotation.EnableCaching
import org.springframework.cache.concurrent.ConcurrentMapCacheManager
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.scheduler.Schedulers
import java.time.Duration

@EnableCaching
@SpringBootApplication
class Application

fun main(args: Array<String>) {
    runApplication<Application>(*args)
}

@Bean
fun cacheManager(): CacheManager {
    return ConcurrentMapCacheManager()
}

data class Comment(
        val postId: Int,
        val id: Int,
        val name: String,
        val email: String,
        val body: String
)

data class Post(
        val userId: Int,
        val id: Int,
        val title: String,
        val body: String
)

data class Response(
        val postId: Int,
        val title: String,
        val user: User,
        val comments: List<LightComment>
)

data class LightComment(
        val email: String,
        val body: String
) {
    constructor(c: Comment) : this(c.email, c.body)
}

data class User(
        val id: Int,
        val name: String,
        val email: String
)

@Service
class ApiService {

    private val client = WebClient.create("http://jsonplaceholder.typicode.com/")

    @Cacheable("comments")
    fun fetchComments(postId: Int) = client
            .get()
            .uri("posts/$postId/comments")
            .retrieve()
            .bodyToFlux(Comment::class.java)
            .cache(Duration.ofMinutes(1))

    fun fetchPosts() = client
            .get()
            .uri("/posts")
            .retrieve()
            .bodyToFlux(Post::class.java)

    @Cacheable("users")
    fun fetchUser(userId: Int) = client
            .get()
            .uri("/users/$userId")
            .retrieve()
            .bodyToMono(User::class.java)
            .cache(Duration.ofMinutes(1))
}

@RestController
@RequestMapping("/api")
class ApiController(
        private val api: ApiService
) {
    @GetMapping
    fun getData() = api.fetchPosts()
            .parallel(4)
            .runOn(Schedulers.elastic())
            .flatMap { post ->
                api.fetchComments(post.id)
                        .map(::LightComment)
                        .collectList()
                        .zipWith(api.fetchUser(post.userId)) { c, u ->
                            Response(post.id, post.title, u, c)
                        }
            }
            .sequential()
}